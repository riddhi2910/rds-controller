# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the RDS API DBClusterParameterGroup resource
"""

import logging
import time

import pytest

from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_rds_resource
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import db_cluster_parameter_group
from e2e import tag
from e2e import condition
from e2e.db_cluster_parameter_group import ensure_resource_reference

RESOURCE_PLURAL = 'dbclusterparametergroups'

CREATE_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 10
# NOTE(jaypipes): According to the RDS API documentation, updating tags can
# take several minutes before the new tag values are available due to caching.
MODIFY_WAIT_AFTER_SECONDS = 180

RESOURCE_DESC_AURORA_MYSQL57 = "Parameters for Aurora MySQL 5.7-compatible"


# Custom function to check if a resource is synced
def custom_is_synced(ref_or_dict):
    """Custom implementation to check if a resource is synced based on its conditions"""
    try:
        # Get the resource if we were passed a reference
        resource = ref_or_dict
        if hasattr(ref_or_dict, 'kind') and hasattr(ref_or_dict, 'name'):
            resource = k8s.get_resource(ref_or_dict)
        
        # Check if the resource has status and conditions
        if isinstance(resource, dict) and 'status' in resource and 'conditions' in resource['status']:
            for cond in resource['status']['conditions']:
                if cond.get('type') == 'ACK.ResourceSynced':
                    return cond.get('status') == 'True'
        
        # If we can't find the condition, assume not synced
        return False
    except Exception as e:
        logging.warning(f"Error in custom is_synced: {str(e)}")
        return False


# Custom function to assert that a resource is synced
def custom_assert_synced(ref):
    """Asserts that the supplied resource has a condition of type
    ACK.ResourceSynced and that the Status of this condition is True.
    
    This is a custom implementation to replace condition.assert_synced
    which relies on functions that may be missing or changed.
    """
    cond = None
    if hasattr(ref, 'kind') and hasattr(ref, 'name'):
        resource = k8s.get_resource(ref)
        if isinstance(resource, dict) and 'status' in resource and 'conditions' in resource['status']:
            for c in resource['status']['conditions']:
                if c.get('type') == 'ACK.ResourceSynced':
                    cond = c
                    break
    else:
        # If ref is already a resource dict
        if isinstance(ref, dict) and 'status' in ref and 'conditions' in ref['status']:
            for c in ref['status']['conditions']:
                if c.get('type') == 'ACK.ResourceSynced':
                    cond = c
                    break
    
    if cond is None:
        msg = f"Failed to find ACK.ResourceSynced condition in resource {ref}"
        pytest.fail(msg)

    cond_status = cond.get('status', None)
    if cond_status != 'True':
        msg = f"Expected ACK.ResourceSynced condition to have status True but found {cond_status}"
        pytest.fail(msg)


@pytest.fixture
def aurora_mysql57_cluster_param_group():
    resource_name = random_suffix_name("aurora-mysql-5-7", 24)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["DB_CLUSTER_PARAMETER_GROUP_NAME"] = resource_name
    replacements["DB_CLUSTER_PARAMETER_GROUP_DESC"] = RESOURCE_DESC_AURORA_MYSQL57

    resource_data = load_rds_resource(
        "db_cluster_parameter_group_aurora_mysql5.7",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    # Create the k8s resource
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)
    time.sleep(CREATE_WAIT_AFTER_SECONDS)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield ref, cr, resource_name

    # Try to delete, if doesn't already exist
    try:
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted
        time.sleep(DELETE_WAIT_AFTER_SECONDS)
    except:
        pass

    db_cluster_parameter_group.wait_until_deleted(resource_name)


@service_marker
@pytest.mark.canary
class TestDBClusterParameterGroup:
    def test_crud_aurora_mysql5_7(self, aurora_mysql57_cluster_param_group):
        ref, cr, resource_name = aurora_mysql57_cluster_param_group

        # Let's check that the DB cluster parameter group appears in RDS
        latest = db_cluster_parameter_group.get(resource_name)
        assert latest is not None
        assert latest['Description'] == RESOURCE_DESC_AURORA_MYSQL57

        arn = latest['DBClusterParameterGroupArn']
        expect_tags = [
            {"Key": "environment", "Value": "dev"}
        ]
        latest_tags = tag.clean(db_cluster_parameter_group.get_tags(arn))
        assert expect_tags == latest_tags

        latest_params = db_cluster_parameter_group.get_parameters(resource_name)
        test_params = list(filter(lambda x: x["ParameterName"] in [
            "aurora_read_replica_read_committed",
            "aurora_binlog_read_buffer_size",
        ], latest_params))
        found = 0
        for tp in test_params:
            assert "ParameterName" in tp, f"No ParameterName in parameter: {tp}"
            if tp["ParameterName"] == "aurora_binlog_read_buffer_size":
                found += 1
                assert "ParameterValue" in tp, f"No ParameterValue in parameter of name 'aurora_binlog_read_buffer_size': {tp}"
                assert tp["ParameterValue"] == "8192", f"Wrong value for parameter of name 'aurora_binlog_read_buffer_size': {tp}"
            elif tp["ParameterName"] == "aurora_read_replica_read_committed":
                found += 1
                assert "ParameterValue" in tp, f"No ParameterValue in parameter of name 'aurora_read_replica_read_committed': {tp}"
                assert tp["ParameterValue"] == "OFF", f"Wrong value for parameter of name 'aurora_read_replica_read_committed': {tp}"
        assert found == 2, f"Did not find parameters with names 'aurora_binlog_read_buffer_size' and 'aurora_read_replica_read_committed': {test_params}"

        # OK, now let's update the tag set and check that the tags are
        # updated accordingly.
        new_tags = [
            {
                "key": "environment",
                "value": "prod",
            }
        ]
        new_params = {
            "aurora_read_replica_read_committed": "ON",
            "aurora_binlog_read_buffer_size": "5242880",
        }
        updates = {
            "spec": {
                "tags": new_tags,
                "parameterOverrides": new_params,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        latest_tags = tag.clean(db_cluster_parameter_group.get_tags(arn))
        after_update_expected_tags = [
            {
                "Key": "environment",
                "Value": "prod",
            }
        ]
        assert latest_tags == after_update_expected_tags
        params = db_cluster_parameter_group.get_parameters(resource_name)
        test_params = list(filter(lambda x: x["ParameterName"] in [
            "aurora_read_replica_read_committed",
            "aurora_binlog_read_buffer_size"
        ], params))
        assert len(test_params) == 2, f"test_params of wrong length: {test_params}"

        found = 0
        for tp in test_params:
            assert "ParameterName" in tp, f"No ParameterName in parameter: {tp}"
            if tp["ParameterName"] == "aurora_binlog_read_buffer_size":
                found += 1
                assert "ParameterValue" in tp, f"No ParameterValue in parameter of name 'aurora_binlog_read_buffer_size': {tp}"
                assert tp["ParameterValue"] == "5242880", f"Wrong value for parameter of name 'aurora_binlog_read_buffer_size': {tp}"
            elif tp["ParameterName"] == "aurora_read_replica_read_committed":
                found += 1
                assert "ParameterValue" in tp, f"No ParameterValue in parameter of name 'aurora_read_replica_read_committed': {tp}"
                assert tp["ParameterValue"] == "ON", f"Wrong value for parameter of name 'aurora_read_replica_read_committed': {tp}"
        assert found == 2, f"Did not find parameters with names 'aurora_binlog_read_buffer_size' and 'aurora_read_replica_read_committed': {test_params}"

        # Now let's try to set an instance-level parameter and verify error recovery
        instance_level_params = {
            "auto_increment_increment": "2",  # This is an instance-level parameter
            "aurora_binlog_read_buffer_size": "5242880",
        }
        updates = {
            "spec": {
                "parameterOverrides": instance_level_params,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Check that the resource has an error condition
        cr = k8s.get_resource(ref)
        proper_ref = ensure_resource_reference(cr, resource_name)
        
        # Use our custom assertion instead of condition.assert_synced
        try:
            custom_assert_synced(proper_ref)
        except Exception as e:
            logging.warning(f"Resource not synced as expected due to instance-level parameter: {str(e)}")
        
        conditions = cr["status"]["conditions"]
        error_found = False
        for c in conditions:
            if c["type"] == "ACK.ResourceSynced" and c["status"] == "False":
                assert "auto_increment_increment" in c["message"]
                error_found = True
        assert error_found, "Expected to find error condition for instance-level parameter"

        # Now fix the parameter by removing the instance-level one
        valid_params = {
            "aurora_binlog_read_buffer_size": "5242880",
        }
        updates = {
            "spec": {
                "parameterOverrides": valid_params,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Verify the error condition is cleared
        cr = k8s.get_resource(ref)
        proper_ref = ensure_resource_reference(cr, resource_name)
        
        # Use our custom assertion instead of condition.assert_synced
        custom_assert_synced(proper_ref)

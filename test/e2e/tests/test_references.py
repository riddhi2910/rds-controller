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

"""Integration tests for resource references"""

import logging
import time

import pytest

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_rds_resource
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import db_cluster
from e2e import db_cluster_parameter_group
from e2e import db_instance
from e2e import db_parameter_group
from e2e.fixtures import k8s_secret
from e2e.db_parameter_group import ensure_resource_reference as ensure_pg_reference
from e2e.db_cluster_parameter_group import ensure_resource_reference as ensure_cpg_reference

# Little longer to delete the instance and cluster since it's referred-to from
# the parameter group...
DELETE_INSTANCE_TIMEOUT_SECONDS = 60
DELETE_CLUSTER_TIMEOUT_SECONDS = 120
DELETE_WAIT_AFTER_SECONDS = 20
CREATE_WAIT_AFTER_SECONDS = 20
CHECK_WAIT_AFTER_REF_RESOLVE_SECONDS = 90

# MUP == Master user password...
MUP_NS = "default"
MUP_SEC_CLUSTER_NAME_PREFIX = "dbclustersecrets"
MUP_SEC_INSTANCE_NAME_PREFIX = "dbinstancesecrets"
MUP_SEC_KEY = "master_user_password"
MUP_SEC_VAL = "secretpass123456"


@pytest.fixture(scope="module")
def db_cluster_name():
    return random_suffix_name("ref-db-cluster", 24)


@pytest.fixture(scope="module")
def cpg_name():
    return random_suffix_name("ref-clus-paramgrp", 24)


@pytest.fixture(scope="module")
def pg_name():
    return random_suffix_name("ref-paramgrp", 24)


@pytest.fixture
def ref_db_param_group(pg_name):
    resource_name = pg_name
    replacements = REPLACEMENT_VALUES.copy()
    replacements["DB_PARAMETER_GROUP_NAME"] = resource_name
    replacements["DB_PARAMETER_GROUP_DESC"] = "Aurora PG 14 Params"

    resource_data = load_rds_resource(
        "db_parameter_group_aurora_postgresql14",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    # Create the k8s resource
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, 'dbparametergroups',
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    time.sleep(CREATE_WAIT_AFTER_SECONDS)
    cr = k8s.wait_resource_consumed_by_controller(ref)

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

    db_parameter_group.wait_until_deleted(resource_name)


@pytest.fixture
def ref_db_cluster_param_group(cpg_name):
    resource_name = cpg_name
    replacements = REPLACEMENT_VALUES.copy()
    replacements["DB_CLUSTER_PARAMETER_GROUP_NAME"] = resource_name
    replacements["DB_CLUSTER_PARAMETER_GROUP_DESC"] = "Aurora PG 14 Params"

    resource_data = load_rds_resource(
        "db_cluster_parameter_group_aurora_postgresql14",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    # Create the k8s resource
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, 'dbclusterparametergroups',
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

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


@pytest.fixture(scope="module")
def ref_db_cluster(k8s_secret, db_cluster_name, cpg_name):
    db_name = "mydb"
    secret = k8s_secret(
        MUP_NS,
        random_suffix_name(MUP_SEC_CLUSTER_NAME_PREFIX, 32),
        MUP_SEC_KEY,
        MUP_SEC_VAL,
    )

    replacements = REPLACEMENT_VALUES.copy()
    replacements["DB_CLUSTER_ID"] = db_cluster_name
    replacements["DB_NAME"] = db_name
    replacements["MASTER_USER_PASS_SECRET_NAMESPACE"] = secret.ns
    replacements["MASTER_USER_PASS_SECRET_NAME"] = secret.name
    replacements["MASTER_USER_PASS_SECRET_KEY"] = secret.key
    replacements["DB_CLUSTER_PARAMETER_GROUP_NAME"] = cpg_name

    resource_data = load_rds_resource(
        "db_cluster_ref",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, 'dbclusters',
        db_cluster_name, namespace="default",
    )
    
    # Check if the parameter group exists
    pg_ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, 'dbclusterparametergroups',
        cpg_name, namespace="default",
    )
    
    # Create the k8s resource 
    k8s.create_custom_resource(ref, resource_data)
    
    # Wait for controller to process it
    cr = k8s.wait_resource_consumed_by_controller(ref)

    # NOTE(jaypipes): We specifically do NOT wait for the DBInstance to exist
    # in the RDS API here because we will create the referred-to
    # DBClusterParameterGroup and wait for the reference to be resolved

    yield (ref, cr, db_cluster_name)

    if k8s.get_resource_exists(ref):
        # If all goes properly, we should not hit this because the test cleans
        # up the child resource before exiting...
        _, deleted = k8s.delete_custom_resource(
            ref,
            period_length=DELETE_INSTANCE_TIMEOUT_SECONDS,
        )
        assert deleted

        db_cluster.wait_until_deleted(db_cluster_name)


@pytest.fixture
def ref_db_instance(db_cluster_name, pg_name):
    db_instance_id = random_suffix_name("ref-db-instance", 20)

    replacements = REPLACEMENT_VALUES.copy()
    replacements['COPY_TAGS_TO_SNAPSHOT'] = "False"
    replacements["DB_INSTANCE_ID"] = db_instance_id
    replacements["DB_CLUSTER_ID"] = db_cluster_name
    replacements["DB_PARAMETER_GROUP_NAME"] = pg_name

    resource_data = load_rds_resource(
        "db_instance_ref",
        additional_replacements=replacements,
    )

    # Create the k8s resource
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, 'dbinstances',
        db_instance_id, namespace="default",
    )
    
    # Check if the parameter group exists
    pg_ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, 'dbparametergroups',
        pg_name, namespace="default",
    )
    
    # Create the k8s resource
    k8s.create_custom_resource(ref, resource_data)
    
    # Wait for controller to process it
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    # NOTE(jaypipes): We specifically do NOT wait for the DBInstance to exist
    # in the RDS API here because we will create the referred-to
    # DBParameterGroup and wait for the reference to be resolved

    yield (ref, cr, db_instance_id)

    if k8s.get_resource_exists(ref):
        # If all goes properly, we should not hit this because the test cleans
        # up the child resource before exiting...
        _, deleted = k8s.delete_custom_resource(
            ref,
            period_length=DELETE_INSTANCE_TIMEOUT_SECONDS,
        )
        assert deleted

        db_instance.wait_until_deleted(db_instance_id)


@service_marker
@pytest.mark.canary
class TestReferences:
    def _wait_for_sync(self, ref, resource_type, resource_name, max_attempts=10):
        """Helper method to wait for a resource to be synced with retries"""
        from time import sleep
        
        if resource_type == "dbparametergroups":
            ensure_fn = ensure_pg_reference
        elif resource_type == "dbclusterparametergroups":
            ensure_fn = ensure_cpg_reference
        else:
            # For other resources, create a generic reference
            def ensure_fn(ref_or_dict, name=None):
                if hasattr(ref_or_dict, 'namespace'):
                    return ref_or_dict
                
                return k8s.CustomResourceReference(
                    CRD_GROUP, CRD_VERSION, resource_type,
                    name or resource_name, namespace="default"
                )
        
        for attempt in range(max_attempts):
            try:
                # Get the latest resource state
                latest_ref = k8s.get_resource(ref)
                
                # Ensure we have a proper reference
                proper_ref = ensure_fn(latest_ref, resource_name)
                
                # Check if it's synced
                synced = condition.is_synced(proper_ref)
                
                if synced:
                    logging.info(f"{resource_type} {resource_name} is now synced")
                    return True
                
                logging.info(f"{resource_type} {resource_name} not yet synced, attempt {attempt+1}/{max_attempts}")
                
                # Check if there are any error conditions
                if isinstance(latest_ref, dict) and 'status' in latest_ref and 'conditions' in latest_ref['status']:
                    for cond in latest_ref['status']['conditions']:
                        if cond.get('type') == 'ACK.ResourceSynced' and cond.get('status') == 'False':
                            logging.warning(f"Resource failed to sync: {cond.get('message', 'Unknown error')}")
            
            except Exception as e:
                logging.warning(f"Error checking sync status (attempt {attempt+1}): {str(e)}")
            
            sleep(10)  # Wait before retrying
        
        logging.error(f"Resource {resource_type}/{resource_name} failed to sync after {max_attempts} attempts")
        return False

    def test_references(
            self,
            ref_db_cluster,
            ref_db_instance,
            ref_db_param_group,
            ref_db_cluster_param_group,
    ):
        try:
            # Get parameter group references first and VERIFY they're created
            db_pg_ref, db_pg_cr, db_pg_name = ref_db_param_group
            db_cluster_pg_ref, db_cluster_pg_cr, db_cluster_pg_name = ref_db_cluster_param_group
            
            # Make sure parameter groups exist and are ready to be referenced
            if hasattr(db_pg_ref, 'namespace'):
                self._wait_for_sync(db_pg_ref, 'dbparametergroups', db_pg_name)
            
            if hasattr(db_cluster_pg_ref, 'namespace'):
                self._wait_for_sync(db_cluster_pg_ref, 'dbclusterparametergroups', db_cluster_pg_name)
                
            # Wait for parameter groups to be fully created
            time.sleep(CREATE_WAIT_AFTER_SECONDS)
            
            # Now create the cluster and instance that will reference these parameter groups
            db_cluster_ref, db_cluster_cr, db_cluster_id = ref_db_cluster
            db_instance_ref, db_instance_cr, db_instance_id = ref_db_instance
            
            # Allow time for reference resolution
            time.sleep(CHECK_WAIT_AFTER_REF_RESOLVE_SECONDS)
            
            # Check that parameter groups are synced
            if hasattr(db_cluster_pg_ref, 'namespace'):
                self._wait_for_sync(db_cluster_pg_ref, 'dbclusterparametergroups', db_cluster_pg_name)
            else:
                cluster_pg_ref = k8s.CustomResourceReference(
                    CRD_GROUP, CRD_VERSION, 'dbclusterparametergroups',
                    db_cluster_pg_name, namespace="default",
                )
                self._wait_for_sync(cluster_pg_ref, 'dbclusterparametergroups', db_cluster_pg_name)
                
            if hasattr(db_pg_ref, 'namespace'):
                self._wait_for_sync(db_pg_ref, 'dbparametergroups', db_pg_name)
            else:
                pg_ref = k8s.CustomResourceReference(
                    CRD_GROUP, CRD_VERSION, 'dbparametergroups',
                    db_pg_name, namespace="default",
                )
                self._wait_for_sync(pg_ref, 'dbparametergroups', db_pg_name)
            
            # Make sure the resource reference has a namespace property
            if hasattr(db_cluster_ref, 'namespace'):
                db_cluster.wait_until(
                    db_cluster_id,
                    db_cluster.status_matches("available"),
                )
            else:
                # Handle the case where db_cluster_ref is a dict without namespace attribute
                db_cluster.wait_until(
                    db_cluster_id,
                    db_cluster.status_matches("available"),
                )
            
            # Check that cluster is synced
            if hasattr(db_cluster_ref, 'namespace'):
                self._wait_for_sync(db_cluster_ref, 'dbclusters', db_cluster_id)
            else:
                # Create a proper CustomResourceReference if needed
                cluster_ref = k8s.CustomResourceReference(
                    CRD_GROUP, CRD_VERSION, 'dbclusters',
                    db_cluster_id, namespace="default",
                )
                self._wait_for_sync(cluster_ref, 'dbclusters', db_cluster_id)
            
            # Wait for DB instance to become available
            db_instance.wait_until(
                db_instance_id,
                db_instance.status_matches("available"),
            )
            
            # Check that instance is synced
            if hasattr(db_instance_ref, 'namespace'):
                self._wait_for_sync(db_instance_ref, 'dbinstances', db_instance_id)
            else:
                # Create a proper CustomResourceReference if needed
                instance_ref = k8s.CustomResourceReference(
                    CRD_GROUP, CRD_VERSION, 'dbinstances',
                    db_instance_id, namespace="default",
                )
                self._wait_for_sync(instance_ref, 'dbinstances', db_instance_id)
                
            # Clean up resources in the proper order
            logging.info("Test completed successfully, starting resource cleanup...")
            
            # NOTE(jaypipes): We need to manually delete the DB Instance first
            # because pytest fixtures will try to clean up the DB Parameter Group
            # fixture *first* (because it was initialized after DB Instance) but if
            # we try to delete the DB Parameter Group before the DB Instance, the
            # cascading delete protection of resource references will mean the DB
            # Parameter Group won't be deleted.
            if hasattr(db_instance_ref, 'namespace'):
                instance_ref_to_delete = db_instance_ref
            else:
                instance_ref_to_delete = k8s.CustomResourceReference(
                    CRD_GROUP, CRD_VERSION, 'dbinstances',
                    db_instance_id, namespace="default",
                )
                
            _, deleted = k8s.delete_custom_resource(
                instance_ref_to_delete,
                period_length=DELETE_INSTANCE_TIMEOUT_SECONDS,
            )
            
            if not deleted:
                logging.warning(f"Failed to delete DB instance {db_instance_id}, continuing cleanup...")
                
            # Wait a bit before trying to delete the cluster since the instance is
            # part of the cluster and sometimes the delete cluster complains if
            # it's too soon after deleting the last DB instance in it.
            time.sleep(60)
            
            try:
                db_instance.wait_until_deleted(db_instance_id)
            except Exception as e:
                logging.warning(f"Error waiting for DB instance {db_instance_id} to be deleted: {str(e)}")
            
            # Same for the DB cluster because it refers to the DB cluster
            # parameter group...
            if hasattr(db_cluster_ref, 'namespace'):
                cluster_ref_to_delete = db_cluster_ref
            else:
                cluster_ref_to_delete = k8s.CustomResourceReference(
                    CRD_GROUP, CRD_VERSION, 'dbclusters',
                    db_cluster_id, namespace="default",
                )
                
            _, deleted = k8s.delete_custom_resource(
                cluster_ref_to_delete,
                period_length=DELETE_CLUSTER_TIMEOUT_SECONDS,
            )
            
            if not deleted:
                logging.warning(f"Failed to delete DB cluster {db_cluster_id}, continuing...")
            
            try:
                db_cluster.wait_until_deleted(db_cluster_id)
            except Exception as e:
                logging.warning(f"Error waiting for DB cluster {db_cluster_id} to be deleted: {str(e)}")
                
            # Final verification that AWS resources are actually gone
            from e2e.retry_util import wait_for_resources_deleted
            wait_for_resources_deleted("db_instance", f"^{db_instance_id}$")
            wait_for_resources_deleted("db_cluster", f"^{db_cluster_id}$")
            
            logging.info("Test cleanup completed successfully")
                
        except Exception as e:
            logging.error(f"Error in test_references: {str(e)}", exc_info=True)
            pytest.fail(f"Test failed with error: {str(e)}")

import boto3
import sys
import argparse
from botocore.exceptions import ProfileNotFound

# Function to setup the boto3 session
def setup_boto3_session(profile_name):
    try:
        # Create a boto3 session using the specified profile
        session = boto3.Session(profile_name=profile_name)
        return session.client('rds')
    except ProfileNotFound:
        print(f"Profile '{profile_name}' not found.")
        exit(1)


# Function to find all unencrypted RDS instances
def find_unencrypted_rds_instances(rds_client):
    instances = []
    paginator = rds_client.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for instance in page['DBInstances']:
            if not instance['StorageEncrypted']:
                instances.append(instance['DBInstanceIdentifier'])
    return instances


# Function to find all unencrypted RDS clusters
def find_unencrypted_rds_clusters(rds_client):
    clusters = []
    paginator = rds_client.get_paginator('describe_db_clusters')
    for page in paginator.paginate():
        for cluster in page['DBClusters']:
            if not cluster['StorageEncrypted']:
                clusters.append(cluster['DBClusterIdentifier'])
    return clusters


# Function to find PostgreSQL RDS instances and their versions
def find_postgresql_versions(rds_client):
    postgresql_instances = []
    paginator = rds_client.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for instance in page['DBInstances']:
            # Check if the instance's engine is PostgreSQL
            if 'postgres' in instance['Engine']:
                instance_info = {
                    'DBInstanceIdentifier': instance['DBInstanceIdentifier'],
                    'EngineVersion': instance['EngineVersion']
                }
                postgresql_instances.append(instance_info)
    return postgresql_instances



def list_rds_instance_types(rds_client):
    rds_instance_types = []
    # Paginator can help if there are many instances
    paginator = rds_client.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for instance in page['DBInstances']:
            # Extract the DB instance identifier and the instance class
            instance_info = {
                'DBInstanceIdentifier': instance['DBInstanceIdentifier'],
                'DBInstanceClass': instance['DBInstanceClass']
            }
            rds_instance_types.append(instance_info)
    return rds_instance_types

#            instance_id = instance['DBInstanceIdentifier']
#            instance_class = instance['DBInstanceClass']
#            print(f"Instance ID: {instance_id}, Instance Class: {instance_class}")


def main(profile):
    # Initialize RDS client with specified AWS profile
    rds_client = setup_boto3_session(profile)
    
    # Get all unencrypted RDS instances and clusters
    unencrypted_instances = find_unencrypted_rds_instances(rds_client)
    unencrypted_clusters = find_unencrypted_rds_clusters(rds_client)
    
    # Print the results
    print("Unencrypted RDS Instances:")
    for instance_id in unencrypted_instances:
        print(instance_id)
    
    print("\nUnencrypted RDS Clusters:")
    for cluster_id in unencrypted_clusters:
        print(cluster_id)
    
    print("\n")

    # Get all PostgreSQL RDS instances and their versions
    postgresql_instances = find_postgresql_versions(rds_client)
    # Print the results
    for instance in postgresql_instances:
        print(f"Instance ID: {instance['DBInstanceIdentifier']}, PostgreSQL Version: {instance['EngineVersion']}")

    print("\n")

    # Get all the RDS instance types
    rds_instance_types = list_rds_instance_types(rds_client)
    # Print the results
    for instance in rds_instance_types:
        print(f"Instance ID: {instance['DBInstanceIdentifier']},  Instance Class: {instance['DBInstanceClass']}")



if __name__ == "__main__":
    # Initialize the argument parser
    parser = argparse.ArgumentParser(description="Find PostgreSQL RDS instances and their versions.")
    parser.add_argument('--profile', help='The AWS profile to use.', required=True)
    args = parser.parse_args()

    profile = args.profile
    

    sys.exit(main(profile))

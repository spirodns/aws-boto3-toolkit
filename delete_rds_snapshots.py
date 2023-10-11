
import boto3 
from botocore.exceptions import ClientError
import logging

aws_profile='box-dev'
#aws_profile='box-prod-eu'
#aws_profile='box-prod-ap'
#aws_profile='box-prod-us'
#aws_profile='acxiom'
#aws_profile='site-jap'

boto3.setup_default_session(profile_name=aws_profile)


# Create logger
logger = logging.getLogger('my_app')
logger.setLevel(logging.DEBUG)

# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Create file handler and set level to debug
fh = logging.FileHandler('encrypt.log')
fh.setLevel(logging.DEBUG)

# Add formatter to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(ch)
logger.addHandler(fh)

# Log messages
logger.debug('This is a debug message')
logger.info('This is an info message')

def delete_rds_db_snapshots(db_snapshot_identifier):

    rds_client = boto3.client('rds')
    
    try:
        response = rds_client.delete_db_snapshot(
            DBSnapshotIdentifier=db_snapshot_identifier
        )
        logger.info(f"Deleting DB snapshot: {response['DBSnapshot']['DBSnapshotIdentifier']}")
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidDBSnapshotStateFault':
            logger.info(f"ERROR: The DB snapshot {db_snapshot_identifier}is not in a valid state for deletion.")
        elif error_code == 'DBSnapshotNotFound':
            logger.info(f"ERROR: The specified DB snapshot {db_snapshot_identifier} could not be found for deletion.")
        else:
            logger.info(f"ERROR: For {db_snapshot_identifier} an unexpected error occurred: {e}")


def delete_rds_cluster_snapshots(cluster_snapshot_identifier):

    rds_client = boto3.client('rds')
    
    try:
        response = rds_client.delete_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=cluster_snapshot_identifier
        )
        logger.info(f"Deleting DB cluster snapshot: {response['DBClusterSnapshot']['DBClusterSnapshotIdentifier']}")
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidDBClusterSnapshotStateFault':
            logger.info(f"ERROR: The DB cluster snapshot {cluster_snapshot_identifier} is not in a valid state for deletion.")
        elif error_code == 'DBClusterSnapshotNotFoundFault':
            logger.info(f"ERROR: The specified DB cluster snapshot {cluster_snapshot_identifier} could not be found for deletion.")
        else:
            logger.info(f"ERROR: For {cluster_snapshot_identifier} an unexpected error occurred: {e}")

def main():


#    db_snapshots_list = list_rds_db_snapshots()
#    cluster_snapshots_list = list_rds_cluster_snapshots()
#    list_kms_keys()
    # Usage
    # source_db_snapshot_identifier='looker-db-staging-eu-west-1-20141013-spiro'
    # source_db_snapshot_identifier=['bxrealtimeanalyticsstack-snapshot-analyticsrealtimedbees-t3ci8jfboxtj','boxeverrealtimeanalyticsorchestrationstack-snapshot-databaseinstanceboxeverrealtimeanalyticsorchestrationstack62a656eb-u1uhgcsjfpus','boxeverrealtimeanalyticsorchestrationstack-snapshot-databaseinstance24d16791-gex5afirp14x','boxeverrealtimeanalyticsorchestrationstack-snapshot-databaseboxeverrealtimeanalyticsorchestrationstack8659871a-1nrmqrby72jco','analyticsdbstack-snapshot-databaseb269d8bb-v3ljdzb1yqba','analyticsdbstack-snapshot-databaseb269d8bb-tv6z0ylektfk','analyticsdbstack-snapshot-databaseb269d8bb-sqthjmihw5nf','analyticsdbstack-snapshot-databaseb269d8bb-ggf2rvarid0a','analyticsdbstack-snapshot-databaseb269d8bb-plbeqf0duqpa','analyticsdbstack-snapshot-databaseb269d8bb-shlmyadm1a9e','analyticsdbstack-snapshot-databaseb269d8bb-4wnximvbfpjr','analyticsdbstack-dev-eu-west-1-snapshot-databaseb269d8bb-xuhmzpqkcsqs','analyticsdbstack-snapshot-databaseb269d8bb-earovncvg0zd','analyticsdbstack-snapshot-databaseb269d8bb-fndpzziqepuq']
    source_db_snapshot_identifier=['']
    for snapshot in source_db_snapshot_identifier:
        delete_rds_db_snapshots(snapshot)
    
    
if __name__ == "__main__":
    main()

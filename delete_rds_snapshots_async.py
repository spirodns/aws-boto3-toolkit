import asyncio
import boto3
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError

client = boto3.client('rds')
executor = ThreadPoolExecutor()


# Create logger
logger = logging.getLogger('my_app')
logger.setLevel(logging.DEBUG)

# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Create file handler and set level to debug
fh = logging.FileHandler('delete.log')
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













def check_rds_db_snapshot_status(rds_db_snapshot_identifier):
    rds_client = boto3.client('rds')

    status_response = ''
    try:
        logger.debug(f"rds_db_snapshot_identifier {rds_db_snapshot_identifier}")
        response = rds_client.describe_db_snapshots(
            DBSnapshotIdentifier=rds_db_snapshot_identifier
        )
        for info in response['DBSnapshots']:
            status_response = info['Status']
            logger.info(f"{rds_db_snapshot_identifier} is in state {status_response}")
            # status_dictionary[rds_db_snapshot_identifier] = status_response
        return status_response
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'DBSnapshotNotFound':
            logger.info(f"ERROR: The specified DB snapshot {rds_db_snapshot_identifier} could not be found for inspection.")
        else:
            logger.info(f"ERROR: For {rds_db_snapshot_identifier} an unexpected error occurred: {e}")


def check_rds_cluster_snapshot_status(rds_cluster_snapshot_identifier):
    rds_client = boto3.client('rds')

    status_response = ''
    try:
        logger.debug(f"rds_cluster_snapshot_identifier {rds_cluster_snapshot_identifier}")
        response = rds_client.describe_db_cluster_snapshots(
            DBSnapshotIdentifier=rds_cluster_snapshot_identifier
        )
        for info in response['DBSnapshots']:
            status_response = info['Status']
            logger.info(f"{rds_cluster_snapshot_identifier} is in state {status_response}")
            # status_dictionary[rds_db_snapshot_identifier] = status_response
        return status_response
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'DBClusterSnapshotNotFound':
            logger.info(f"ERROR: The specified DB Cluster snapshot {rds_cluster_snapshot_identifier} could not be found for inspection.")
        else:
            logger.info(f"ERROR: For {rds_cluster_snapshot_identifier} an unexpected error occurred: {e}")


def delete_rds_db_snapshot(db_snapshot_identifier):

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


def delete_rds_cluster_snapshot(cluster_snapshot_identifier):

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


def delete_rds_cluster_snapshot(cluster_snapshot_identifier):

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





async def delete_unencrypted_db_snapshot(snapshot_id, encrypted_snapshot_id):
    loop = asyncio.get_running_loop()
    
    while True:
        # Check status of encrypted snapshot
        logger.info(f"Checking status of encrypted snapshot {encrypted_snapshot_id}")
        response = await loop.run_in_executor(
            executor,
            check_rds_db_snapshot_status,
            encrypted_snapshot_id
            # client.describe_db_snapshots,
            # DBSnapshotIdentifier=encrypted_snapshot_id
        )
        # status = response['DBSnapshots'][0]['Status']
        status = response
        if status == 'available':
            logger.info(f"Deleting unencrypted db snapshot {snapshot_id}")
            # Delete unencrypted snapshot
            await loop.run_in_executor(
                executor,
                delete_rds_db_snapshot,
                snapshot_id
                # client.delete_db_snapshot,
                # DBSnapshotIdentifier=snapshot_id
            )
            break
        await asyncio.sleep(60)  # Wait for 60 seconds before checking again


async def delete_unencrypted_cluster_snapshot(snapshot_id, encrypted_snapshot_id):
    loop = asyncio.get_running_loop()
    
    while True:
        # Check status of encrypted snapshot
        logger.info(f"Checking status of encrypted snapshot {encrypted_snapshot_id}")
        response = await loop.run_in_executor(
            executor,
            check_rds_cluster_snapshot_status,
            encrypted_snapshot_id
            # client.describe_db_snapshots,
            # DBSnapshotIdentifier=encrypted_snapshot_id
        )
        # status = response['DBSnapshots'][0]['Status']
        status = response
        if status == 'available':
            logger.info(f"Deleting unencrypted cluster db snapshot {snapshot_id}")
            # Delete unencrypted snapshot
            await loop.run_in_executor(
                executor,
                delete_rds_cluster_snapshot,
                snapshot_id
                # client.delete_db_snapshot,
                # DBSnapshotIdentifier=snapshot_id
            )
            break
        await asyncio.sleep(60)  # Wait for 60 seconds before checking again


async def main():

    encrypted_snapshots = ['emr-pre-vpc-move-encrypted',
'emr-production-eu-west-1-final-snapshot-encrypted',
'emr-production-eu-west-1-mreddy-encrypted',
'emr-rds-adhoc-production2-encrypted',
'engage-prod2-eu-feb-1-pre-upgrade-encrypted',
'import-eu-prod-pre-rds-upgrade-encrypted',
'import-pre-rds-upgrade-prod2-feb5-encrypted',
'looker-2016-11-16-encrypted',
'looker-6-db-production-eu-west-1-final-snapshot-encrypted',
'looker-6-snapshot-encrypted',
'looker-db-production-eu-west-1-final-snapshot-encrypted',
'looker-pre-singlenode-encrypted',
'looker-singlenode-duplicate-snapshot-encrypted',
'looker-singlenode-from-172-16-14-61-encrypted',
'old-looker-finalsnapshot-encrypted',
'post-alienvault-alert-encrypted']


    
    tasks = []
    # for snapshot in unencrypted_rds_db_snapshots:
    for snapshot in encrypted_snapshots:
        # snapshot_id = snapshot
        # encrypted_snapshot_id = await create_encrypted_db_copy(snapshot_id)
        print(snapshot)
        snapshot_id = snapshot.removesuffix('-encrypted')
        encrypted_snapshot_id =  snapshot
        tasks.append(delete_unencrypted_db_snapshot(snapshot_id, encrypted_snapshot_id))

    # for snapshot in unencrypted_rds_cluster_snapshots:
    #     snapshot_id = snapshot
    #     encrypted_snapshot_id = await create_encrypted_cluster_copy(snapshot_id)
    #     tasks.append(delete_unencrypted_cluster_snapshot(snapshot_id, encrypted_snapshot_id))
        

    await asyncio.gather(*tasks)

asyncio.run(main())






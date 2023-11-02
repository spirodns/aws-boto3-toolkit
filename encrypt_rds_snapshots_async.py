import asyncio
import boto3
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError

rds_client = boto3.client('rds')
executor = ThreadPoolExecutor()

# Create logger
logger = logging.getLogger('my_app')
logger.setLevel(logging.DEBUG)

# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Create file handler and set level to debug
fh = logging.FileHandler('async.log')
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

def get_or_create_kms_key(alias_name):
    # Initialize the KMS client
    kms_client = boto3.client('kms')
    # Check if the key alias already exists
    try:
        response = kms_client.describe_key(KeyId=f'alias/{alias_name}')
        key_id = response['KeyMetadata']['KeyId']
        logger.info(f"KMS key with alias '{alias_name}' already exists with KeyId: {key_id}")
        return key_id
    except kms_client.exceptions.NotFoundException:
        # If the key alias does not exist, create a new key
        key_description = 'Default master key that protects my RDS database volumes when no other key is defined'
        key_response = kms_client.create_key(
            Description=key_description,
            Origin='AWS_KMS',  # AWS managed key
            KeyUsage='ENCRYPT_DECRYPT',
            BypassPolicyLockoutSafetyCheck=False
        )
        key_id = key_response['KeyMetadata']['KeyId']
        # Create an alias for the new key
        kms_client.create_alias(
            AliasName=f'alias/{alias_name}',
            TargetKeyId=key_id
        )
        logger.info(f"Created new KMS key with alias '{alias_name}' and KeyId: {key_id}")
        return key_id


def list_rds_db_snapshots():

    db_snapshots_list = []
    # Paginator for RDS DB snapshots
    db_snapshot_paginator = rds_client.get_paginator('describe_db_snapshots')
    # Iterate through pages of DB snapshots
    for page in db_snapshot_paginator.paginate(SnapshotType='manual'):
        for snapshot in page['DBSnapshots']:
            if snapshot['Encrypted'] == False:
                logger.debug(f"DBSnapshotIdentifier: {snapshot['DBSnapshotIdentifier']}, Status: {snapshot['Status']}, DBInstanceIdentifier: {snapshot['DBInstanceIdentifier']}, SnapshotType: {snapshot['SnapshotType']}, Engine: {snapshot['Engine']}, SnapshotCreateTime: {snapshot['SnapshotCreateTime']} , Encrypted: {snapshot['Encrypted']}")
               # db_snapshots_list.append(snapshot['DBSnapshotIdentifier'])
                db_snapshot_identifier = snapshot['DBSnapshotIdentifier']
                match = re.search(r'[^:]+$' , db_snapshot_identifier)
                if match:
                    db_snapshot_identifier = match.group(0)
                    db_snapshots_list.append(db_snapshot_identifier)
    return db_snapshots_list


# def encrypt_rds_db_snapshot(source_db_snapshot_identifier,kms_key_id):
def encrypt_rds_db_snapshot(source_db_snapshot_identifier,key_id):
    kms_key_id=key_id

    suffix = 'encrypted'
    # Specify target  RDS DB encrypted snapshot name
    target_encrypted_db_snapshot_identifier = f"{source_db_snapshot_identifier}-{suffix}"
    try:
        # Create a copy of the DB snapshot with encryption enabled
        response = rds_client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=source_db_snapshot_identifier,
            TargetDBSnapshotIdentifier=target_encrypted_db_snapshot_identifier,
            KmsKeyId=kms_key_id
        )
        logger.info(f"Encrypted DB snapshot created: {response['DBSnapshot']['DBSnapshotIdentifier']}")
        encrypted_db_snapshot_id = response['DBSnapshot']['DBSnapshotIdentifier']
        return encrypted_db_snapshot_id
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'DBSnapshotAlreadyExists':
            logger.info(f"ERROR: The DB snapshot {target_encrypted_db_snapshot_identifier} for encryption already exists.")
            return target_encrypted_db_snapshot_identifier
        elif error_code == 'DBSnapshotNotFound':
            logger.info(f"ERROR: The specified DB snapshot {source_db_snapshot_identifier} for encryption could not be found.")
        elif error_code == 'InvalidDBSnapshotState':
            logger.info(f"ERROR: The specified DB snapshot {source_db_snapshot_identifier} for encryption is in invalid state.")
        elif error_code == 'SnapshotQuotaExceeded':
            logger.info(f"ERROR: Snapshot Quota has been Exceeded.")
        elif error_code == 'KMSKeyNotAccessible':
            logger.info(f"ERROR: KMS Key {kms_key_id} Not Accessible.")
        elif error_code == 'CustomAvailabilityZoneNotFound':
            logger.info(f"ERROR: Custom Availability Zone Not Found.")
        else:
            logger.info(f"ERROR: For {source_db_snapshot_identifier} an unexpected error occurred: {e}")


def check_rds_db_snapshot_status(rds_db_snapshot_identifier):

    status_response = ''
    try:
        logger.debug(f"rds_db_snapshot_identifier {rds_db_snapshot_identifier}")
        response = rds_client.describe_db_snapshots(
            DBSnapshotIdentifier=rds_db_snapshot_identifier
        )
        for info in response['DBSnapshots']:
            status_response = info['Status']
            logger.info(f"{rds_db_snapshot_identifier} is in state {status_response}")
            logger.debug(type(status_response))
        return status_response
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'DBSnapshotNotFound':
            logger.info(f"ERROR: The specified DB snapshot {rds_db_snapshot_identifier} could not be found for inspection.")
        else:
            logger.info(f"ERROR: For {rds_db_snapshot_identifier} an unexpected error occurred: {e}")


def delete_rds_db_snapshot(db_snapshot_identifier):

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


async def create_encrypted_db_copy(snapshot_id,key_id):
    loop = asyncio.get_running_loop()
    encrypted_snapshot_id = f"{snapshot_id}-encrypted".replace(':', '-') 
    
    # Start copy operation
    logger.info(f"Start encryption for snapshot_id {snapshot_id}")
    await loop.run_in_executor(
        executor,
        encrypt_rds_db_snapshot,
        snapshot_id,
        key_id
    )
    return encrypted_snapshot_id


async def delete_unencrypted_db_snapshot(snapshot_id, encrypted_snapshot_id):
    loop = asyncio.get_running_loop()
    
    while True:
        # Check status of encrypted snapshot
        logger.info(f"Checking status of encrypted snapshot {encrypted_snapshot_id}")
        response = await loop.run_in_executor(
            executor,
            check_rds_db_snapshot_status,
            encrypted_snapshot_id
        )
        status = response
        if status == 'available':
            logger.info(f"Deleting unencrypted db snapshot {snapshot_id}")
            # Delete unencrypted snapshot
            await loop.run_in_executor(
                executor,
                delete_rds_db_snapshot,
                snapshot_id
            )
            break
        await asyncio.sleep(60)  # Wait for 60 seconds before checking again


async def main():
    key_alias = 'aws/rds'
    key_id = get_or_create_kms_key(key_alias)
    logger.info(f"Use this KeyId for further operations: {key_id}")
    # Fetch unencrypted snapshots
    unencrypted_rds_db_snapshots = list_rds_db_snapshots()

    tasks = []
    for snapshot in unencrypted_rds_db_snapshots:
       snapshot_id = snapshot
       encrypted_snapshot_id = await create_encrypted_db_copy(snapshot_id,key_id)
       tasks.append(delete_unencrypted_db_snapshot(snapshot_id, encrypted_snapshot_id))

    await asyncio.gather(*tasks)

asyncio.run(main())

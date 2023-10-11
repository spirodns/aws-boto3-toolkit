import asyncio
import boto3
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError

client = boto3.client('rds')
executor = ThreadPoolExecutor()

KMS_KEY_ID = '50fb76aa-1217-469a-93d0-5672936fab95'  # replace with your KMS key ID

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

    rds_client = boto3.client('rds')
    db_snapshots_list = []
    # Paginator for RDS DB snapshots
    db_snapshot_paginator = rds_client.get_paginator('describe_db_snapshots')
    # Iterate through pages of DB snapshots
    #for page in db_snapshot_paginator.paginate(IncludeShared=True, IncludePublic=True):
    #for page in db_snapshot_paginator.paginate():
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


def list_rds_cluster_snapshots():

    rds_client = boto3.client('rds')
    cluster_snapshots_list = []
    # Paginator for RDS Cluster snapshots
    cluster_snapshot_paginator = rds_client.get_paginator('describe_db_cluster_snapshots')
    # Iterate through pages of  RDS Cluster snapshots
    #for page in cluster_snapshot_paginator.paginate(IncludeShared=True, IncludePublic=True):
    for page in cluster_snapshot_paginator.paginate():
        for snapshot in page['DBClusterSnapshots']:
            if snapshot['StorageEncrypted'] == False:
                logger.debug(f"DBClusterSnapshotIdentifier: {snapshot['DBClusterSnapshotIdentifier']}, Status: {snapshot['Status']}, DBClusterIdentifier: {snapshot['DBClusterIdentifier']}, SnapshotType: {snapshot['SnapshotType']}, Engine: {snapshot['Engine']}, SnapshotCreateTime: {snapshot['SnapshotCreateTime']} ,Encrypted: {snapshot['StorageEncrypted']}")
                #cluster_snapshots_list.append(snapshot['DBClusterSnapshotIdentifier'])
                cluster_snapshot_identifier = snapshot['DBClusterSnapshotIdentifier']
                match = re.search(r'[^:]+$', cluster_snapshot_identifier)
                if match:
                    cluster_snapshot_identifier = match.group(0)
                    cluster_snapshots_list.append(cluster_snapshot_identifier)
    return cluster_snapshots_list


# def encrypt_rds_db_snapshot(source_db_snapshot_identifier,kms_key_id):
def encrypt_rds_db_snapshot(source_db_snapshot_identifier):
    kms_key_id=KMS_KEY_ID

    rds_client = boto3.client('rds')
    suffix = 'encrypted'
    # Specify target  RDS DB encrypted snapshot name
    print(f"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii source_db_snapshot_identifier: {source_db_snapshot_identifier}")
    #target_encrypted_db_snapshot_identifier = f"{source_db_snapshot_identifier}-{suffix}"
    target_encrypted_db_snapshot_identifier = f"{source_db_snapshot_identifier}-{suffix}".replace(':', '-') 


    print(f"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii target_encrypted_db_snapshot_identifie: {target_encrypted_db_snapshot_identifier}")

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


def encrypt_rds_cluster_snapshot(source_cluster_snapshot_identifier):
    kms_key_id=KMS_KEY_ID
    rds_client = boto3.client('rds')
    suffix = 'encrypted'
    # Specify the RDS Cluster snapshot identifier and target encrypted snapshot name
    target_encrypted_cluster_snapshot_identifier = f"{source_cluster_snapshot_identifier}-{suffix}"
    try:
        # Create a copy of the Cluster snapshot with encryption enabled
        response = rds_client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=source_cluster_snapshot_identifier,
            TargetDBClusterSnapshotIdentifier=target_encrypted_cluster_snapshot_identifier,
            KmsKeyId=kms_key_id
        )
        logger.info(f"Encrypted Cluster snapshot created: {response['DBClusterSnapshot']['DBClusterSnapshotIdentifier']}")
        encrypted_cluster_snapshot_id = response['DBClusterSnapshot']['DBClusterSnapshotIdentifier']
        return encrypted_cluster_snapshot_id
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'DBClusterSnapshotAlreadyExists':
            logger.info(f"ERROR: The Cluster snapshot {target_encrypted_cluster_snapshot_identifier} for encryption already exist.")
        elif error_code == 'DBClusterSnapshotNotFound':
            logger.info(f"ERROR: The specified Cluster snapshot {source_cluster_snapshot_identifier} for encryption could not be found.")
        elif error_code == 'InvalidDBClusterSnapshotState':
            logger.info(f"ERROR: The specified Cluster snapshot {source_cluster_snapshot_identifier} for encryption is in invalid state.")
        elif error_code == 'SnapshotQuotaExceeded':
            logger.info(f"ERROR: Snapshot Quota has been Exceeded.")
        elif error_code == 'KMSKeyNotAccessible':
            logger.info(f"ERROR: KMS Key {kms_key_id} Not Accessible.")
        elif error_code == 'InvalidDBClusterState':
            logger.info(f"ERROR: The DB Cluster for Cluster snapshot {source_cluster_snapshot_identifier} is in Invalid State.")
        else:
            logger.info(f"ERROR: For {source_cluster_snapshot_identifier} an unexpected error occurred: {e}")


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
            logger.debug(type(status_response))
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
            DBClusterSnapshotIdentifier=rds_cluster_snapshot_identifier
        )
        for info in response['DBSnapshots']:
            status_response = info['Status']
            logger.info(f"{rds_cluster_snapshot_identifier} is in state {status_response}")
            logger.debug(type(status_response))
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

async def create_encrypted_db_copy(snapshot_id):
    loop = asyncio.get_running_loop()
    encrypted_snapshot_id = f"{snapshot_id}-encrypted".replace(':', '-') 
    
    # Start copy operation
    logger.info(f"Start encryption for snapshot_id {snapshot_id}")
    await loop.run_in_executor(
        executor,
        encrypt_rds_db_snapshot,
        snapshot_id
        # client.copy_db_snapshot,
        # SourceDBSnapshotIdentifier=snapshot_id,
        # TargetDBSnapshotIdentifier=encrypted_snapshot_id,
        # KmsKeyId=KMS_KEY_ID
    )
    return encrypted_snapshot_id

async def create_encrypted_cluster_copy(snapshot_id):
    loop = asyncio.get_running_loop()
    encrypted_snapshot_id = f"{snapshot_id}-encrypted"
    
    # Start copy operation
    logger.info(f"Start encryption for snapshot_id {snapshot_id}")
    await loop.run_in_executor(
        executor,
        encrypt_rds_cluster_snapshot,
        snapshot_id
        # client.copy_db_snapshot,
        # SourceDBSnapshotIdentifier=snapshot_id,
        # TargetDBSnapshotIdentifier=encrypted_snapshot_id,
        # KmsKeyId=KMS_KEY_ID
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
    key_alias = 'aws/rds'
    key_id = get_or_create_kms_key(key_alias)
    logger.info(f"Use this KeyId for further operations: {key_id}")
    # Fetch unencrypted snapshots
    unencrypted_rds_db_snapshots = list_rds_db_snapshots()

    #unencrypted_rds_db_snapshots = ['looker-db-eu-west-1-dev-2023-10-03-03-08','looker-db-eu-west-1-dev-2023-10-04-03-09']
    #unencrypted_rds_db_snapshots = ['rds:looker-db-eu-west-1-dev-2023-10-03-03-08','rds:looker-db-eu-west-1-dev-2023-10-04-03-09']

    #unencrypted_rds_cluster_snapshots = list_rds_cluster_snapshots()
    #unencrypted_snapshots = client.describe_db_snapshots(Filters=[{'Name': 'encrypted', 'Values': ['false']}])['DBSnapshots']
    # unencrypted_rds_db_snapshots =  ['looker-db100gb-eu-west-1-dev-final-snapshot','looker-db-eu-west-1-dev','looker-db-pre-vpc']
    # unencrypted_rds_cluster_snapshots = ['gp-database-cluster','dv-postgres-repository-unencrypted','dv-postgres-repository-unencrypted']

    
    #tasks = []
    #for snapshot in unencrypted_rds_db_snapshots:
    #    snapshot_id = snapshot
    #    encrypted_snapshot_id = await create_encrypted_db_copy(snapshot_id)
    #    tasks.append(delete_unencrypted_db_snapshot(snapshot_id, encrypted_snapshot_id))

    #for snapshot in unencrypted_rds_cluster_snapshots:
    #     snapshot_id = snapshot
    #     encrypted_snapshot_id = await create_encrypted_cluster_copy(snapshot_id)
    #     tasks.append(delete_unencrypted_cluster_snapshot(snapshot_id, encrypted_snapshot_id))
    #    

    #await asyncio.gather(*tasks)

asyncio.run(main())

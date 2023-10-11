import boto3 
from botocore.exceptions import ClientError
import logging
import pprint

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


#def list_kms_keys():
#    
#    # Initialize the KMS client
#    kms_client = boto3.client('kms')
#    
#    # List KMS keys
#    keys_response = kms_client.list_keys()
#    
#    for key in keys_response['Keys']:
#        key_id = key['KeyId']
#        key_description_response = kms_client.describe_key(KeyId=key_id)
#        key_description = key_description_response['KeyMetadata']['Description']
#        key_alias_response = kms_client.list_aliases(KeyId=key_id)
#        for key in key_alias_response['Aliases']:
#            print(key['AliasName'])
#            key_alias_name=key['AliasName']
#        print(f"KeyId: {key_id}, Alias: {key_alias_name}, Description: {key_description}")



def list_rds_db_snapshots():

    rds_client = boto3.client('rds')
    db_snapshots_list = []
    # Paginator for RDS DB snapshots
    db_snapshot_paginator = rds_client.get_paginator('describe_db_snapshots')
    # Iterate through pages of DB snapshots
    for page in db_snapshot_paginator.paginate(IncludeShared=True, IncludePublic=True):
        for snapshot in page['DBSnapshots']:
            if snapshot['Encrypted'] == False:
                logger.debug(f"DBSnapshotIdentifier: {snapshot['DBSnapshotIdentifier']}, Status: {snapshot['Status']}, DBInstanceIdentifier: {snapshot['DBInstanceIdentifier']}, SnapshotType: {snapshot['SnapshotType']}, Engine: {snapshot['Engine']}, SnapshotCreateTime: {snapshot['SnapshotCreateTime']} , Encrypted: {snapshot['Encrypted']}")
                db_snapshots_list.append(snapshot)
    return db_snapshots_list


def list_rds_cluster_snapshots():

    rds_client = boto3.client('rds')
    cluster_snapshots_list = []
    # Paginator for RDS Cluster snapshots
    cluster_snapshot_paginator = rds_client.get_paginator('describe_db_cluster_snapshots')
    # Iterate through pages of  RDS Cluster snapshots
    for page in cluster_snapshot_paginator.paginate(IncludeShared=True, IncludePublic=True):
        for snapshot in page['DBClusterSnapshots']:
            if snapshot['StorageEncrypted'] == False:
                logger.debug(f"DBClusterSnapshotIdentifier: {snapshot['DBClusterSnapshotIdentifier']}, Status: {snapshot['Status']}, DBClusterIdentifier: {snapshot['DBClusterIdentifier']}, SnapshotType: {snapshot['SnapshotType']}, Engine: {snapshot['Engine']}, SnapshotCreateTime: {snapshot['SnapshotCreateTime']} ,Encrypted: {snapshot['StorageEncrypted']}")
                cluster_snapshots_list.append(snapshot)
    return cluster_snapshots_list

def list_snapshots():
   db_snapshots_list = list_rds_db_snapshots()
   cluster_snapshots_list = list_rds_cluster_snapshots()
   db_snapshots_list.append(cluster_snapshots_list)
   return db_snapshots_list


def encrypt_rds_db_snapshot(source_db_snapshot_identifier,kms_key_id):

    rds_client = boto3.client('rds')
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



def encrypt_rds_cluster_snapshot(source_cluster_snapshot_identifier,kms_key_id):
    
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


def db_snapshots_status_dict(db_snapshots_list):
    db_snapshots_status_dictionary = {}
    for db_snapshot in db_snapshots_list:
        logger.debug(f"DB snapshot {db_snapshot}")
        db_snapshot_state = check_rds_db_snapshot_status(db_snapshot)
        db_snapshots_status_dictionary[db_snapshot] = db_snapshot_state
        logger.debug(f"db_snapshots_status_dictionary {db_snapshots_status_dictionary}")
    return db_snapshots_status_dictionary


def reconciliation(snapshot_list):
    ## Find which encrypted copies are in the 'available' state. 
    ## If both the encrypted copy and the source are in 'available' state, delete the source.
    ## Then remove the copy from the dictionary
    ## check again, until the dictionary is empty

    db_snapshot_dictionary = db_snapshots_status_dict(snapshot_list)
    suffix = "-encrypted"
    while len(db_snapshot_dictionary)!= 0:
        cleanup_list=[]
        for snapshot,status in db_snapshot_dictionary.items():
            logger.info(f"The snapshot {snapshot} is in state {status}")
            if status == 'available':
                source_snapshot = snapshot.rstrip(f"{suffix}")
                logger.info(f"Deleting the source snapshot: {source_snapshot} ...")
                cleanup_list.append(snapshot)
        for snapshot in cleanup_list:
            del db_snapshot_dictionary[snapshot]
    

def main():

    source_db_snapshot_identifier='looker-db-staging-eu-west-1-20141013'
    suffix = 'encrypted'
    # Specify target  RDS DB encrypted snapshot name
    target_encrypted_db_snapshot_identifier = f"{source_db_snapshot_identifier}-{suffix}"
    key_alias = 'aws/rds'
    key_id = get_or_create_kms_key(key_alias)
    logger.info(f"Use this KeyId for further operations: {key_id}")


    ## List existing non-encrypted snapshot
    # db_snapshots_list = list_rds_db_snapshots()
    db_snapshots_list = ['looker-db100gb-eu-west-1-dev-final-snapshot','looker-db-eu-west-1-dev','looker-db-pre-vpc','looker-db-staging-eu-west-1-20141013']
    # cluster_snapshots_list = list_rds_cluster_snapshots()
    ## Send the copy encrypted command for snapshots
    encrypted_db_snapshot_list = []
    for source_db_snapshot_identifier in db_snapshots_list:
        encrypted_db_snapshot_id = encrypt_rds_db_snapshot(source_db_snapshot_identifier,key_id)
        encrypted_db_snapshot_list.append(encrypted_db_snapshot_id)
    ## use filter() to remove None values in list
    encrypted_db_snapshot_list_clean = list(filter(lambda item: item is not None,encrypted_db_snapshot_list))
    logger.debug(f"encrypted_db_snapshot_list {encrypted_db_snapshot_list}")
    # encrypted_cluster_snapshot_list = []
    # for source_cluster_snapshot_identifier in cluster_snapshots_list:
    #     encrypted_cluster_snapshot_id = encrypt_rds_cluster_snapshot(source_cluster_snapshot_identifier,key_id)
    #     encrypted_cluster_snapshot_list.append(encrypted_cluster_snapshot_id)

    ## Reconciliation
    logger.info(f"Checking the state of the snapshots")
    logger.info("Running reconciliation for the existing DB snapshots...")
    reconciliation(encrypted_db_snapshot_list_clean)


    ## Find which encrypted copies are in the 'available' state. 
    ## If both the encrypted copy and the source are in 'available' state, delete the source.
    ## Then remove the copy from the dictionary
    ## check again, until the dictionary is empty

#    source_status = check_rds_db_snapshot_status(source_db_snapshot_identifier)
#    target_status = ''
#    while target_status != 'available':
#        target_status = check_rds_db_snapshot_status(target_encrypted_db_snapshot_identifier)
#
#    if source_status == 'available' and target_status == 'available':
#        logger.debug(f"OK to Delete: {target_encrypted_db_snapshot_identifier}")
#        delete_rds_db_snapshots(target_encrypted_db_snapshot_identifier)
#    delete_rds_db_snapshots(target_encrypted_db_snapshot_identifier)


#    target_db_snapshot_identifier='looker-db-staging-eu-west-1-20141013-encrypted'
    
    
if __name__ == "__main__":
    main()

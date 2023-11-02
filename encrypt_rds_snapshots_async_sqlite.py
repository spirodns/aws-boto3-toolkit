import boto3
import sqlite3
import asyncio
import logging
# import re
from botocore.exceptions import ClientError

rds = boto3.client('rds')

# Create logger
logger = logging.getLogger('my_app')
logger.setLevel(logging.DEBUG)

# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Create file handler and set level to debug
fh = logging.FileHandler('async-sqlite.log')
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

# Setting up SQLite
conn = sqlite3.connect('rds_snapshots.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS snapshots (
        original_snapshot_id TEXT,
        encrypted_snapshot_id TEXT,
        status TEXT
    )
''')
conn.commit()

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
    for page in db_snapshot_paginator.paginate(SnapshotType='manual'):
        for snapshot in page['DBSnapshots']:
            if snapshot['Encrypted'] == False:
                logger.debug(f"DBSnapshotIdentifier: {snapshot['DBSnapshotIdentifier']}, Status: {snapshot['Status']}, DBInstanceIdentifier: {snapshot['DBInstanceIdentifier']}, SnapshotType: {snapshot['SnapshotType']}, Engine: {snapshot['Engine']}, SnapshotCreateTime: {snapshot['SnapshotCreateTime']} , Encrypted: {snapshot['Encrypted']}")
                db_snapshot_identifier = snapshot['DBSnapshotIdentifier']
                db_snapshots_list.append(db_snapshot_identifier)
    return db_snapshots_list

async def encrypt_snapshot(snapshot_id,key_id):
    encrypted_snapshot_id = snapshot_id + '-encrypted'
    # Initiate copy to create encrypted snapshot
    try:
        rds.copy_db_snapshot(
            SourceDBSnapshotIdentifier=snapshot_id,
            TargetDBSnapshotIdentifier=encrypted_snapshot_id,
            KmsKeyId=key_id,
            CopyTags=True
        )
        cursor.execute("INSERT INTO snapshots VALUES (?, ?, 'copying')", (snapshot_id, encrypted_snapshot_id))
        conn.commit()
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'DBSnapshotAlreadyExists':
            logger.info(f"ERROR: The DB snapshot {encrypted_snapshot_id} for encryption already exists.")
        elif error_code == 'DBSnapshotNotFound':
            logger.info(f"ERROR: The specified DB snapshot {snapshot_id} for encryption could not be found.")
        elif error_code == 'InvalidDBSnapshotState':
            logger.info(f"ERROR: The specified DB snapshot {snapshot_id} for encryption is in invalid state.")
        elif error_code == 'SnapshotQuotaExceeded':
            logger.info(f"ERROR: Snapshot Quota has been Exceeded.")
        elif error_code == 'KMSKeyNotAccessible':
            logger.info(f"ERROR: KMS Key {key_id} Not Accessible.")
        elif error_code == 'CustomAvailabilityZoneNotFound':
            logger.info(f"ERROR: Custom Availability Zone Not Found.")
        else:
            logger.info(f"ERROR: For {snapshot_id} an unexpected error occurred: {e}")

async def check_and_delete_snapshot():
    while True:
        await asyncio.sleep(60)  # check every minute
        rows = cursor.execute("SELECT original_snapshot_id, encrypted_snapshot_id FROM snapshots WHERE status='copying'").fetchall()
        for row in rows:
            encrypted_snapshot = rds.describe_db_snapshots(DBSnapshotIdentifier=row[1])
            if encrypted_snapshot['DBSnapshots'][0]['Status'] == 'available':
                rds.delete_db_snapshot(DBSnapshotIdentifier=row[0])  # delete unencrypted snapshot
                cursor.execute("UPDATE snapshots SET status='available' WHERE original_snapshot_id=?", (row[0],))
                conn.commit()

async def main():
    
    key_alias = 'aws/rds'
    key_id = get_or_create_kms_key(key_alias)
    logger.info(f"Use this KeyId for further operations: {key_id}")

    # Identify unencrypted snapshots
    snapshots = list_rds_db_snapshots()

    tasks = [encrypt_snapshot(snapshot,key_id) for snapshot in snapshots]

    # Start encryption and monitoring
    await asyncio.gather(*tasks, check_and_delete_snapshot())

asyncio.run(main())


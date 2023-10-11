import asyncio
import boto3
import logging
import pprint
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
fh = logging.FileHandler('list.log')
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

def list_rds_db_snapshots():

    rds_client = boto3.client('rds')
    db_snapshots_list = []
    # Paginator for RDS DB snapshots
    db_snapshot_paginator = rds_client.get_paginator('describe_db_snapshots')
    # Iterate through pages of DB snapshots
    #for page in db_snapshot_paginator.paginate(IncludeShared=True, IncludePublic=True):
    for page in db_snapshot_paginator.paginate():
        for snapshot in page['DBSnapshots']:
            if snapshot['Encrypted'] == False:
                logger.debug(f"DBSnapshotIdentifier: {snapshot['DBSnapshotIdentifier']}, Status: {snapshot['Status']}, DBInstanceIdentifier: {snapshot['DBInstanceIdentifier']}, SnapshotType: {snapshot['SnapshotType']}, Engine: {snapshot['Engine']}, SnapshotCreateTime: {snapshot['SnapshotCreateTime']} , Encrypted: {snapshot['Encrypted']}")
#                db_snapshots_list.append(snapshot)
                db_snapshot_identifier = (snapshot['DBSnapshotIdentifier'])
                match = re.search(r'[^:]+$' , db_snapshot_identifier)
                if match:
                    db_snapshot_identifier = match.group(0)
                    db_snapshots_list.append(db_snapshot_identifier)
#                db_snapshots_list.append(snapshot['DBSnapshotIdentifier'])
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


def main():
    rds_db_snapshots_list = list_rds_db_snapshots()
    for snap in rds_db_snapshots_list:
        pprint.pprint(snap)
#    list_rds_cluster_snapshots()


if __name__ == "__main__":
    main()

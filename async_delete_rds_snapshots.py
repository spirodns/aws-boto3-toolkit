import asyncio
import boto3
import logging
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

    # unencrypted_rds_cluster_snapshots = list_rds_cluster_snapshots()
    #unencrypted_snapshots = client.describe_db_snapshots(Filters=[{'Name': 'encrypted', 'Values': ['false']}])['DBSnapshots']
    # unencrypted_rds_db_snapshots =  ['looker-db100gb-eu-west-1-dev-final-snapshot','looker-db-eu-west-1-dev','looker-db-pre-vpc']
    # unencrypted_rds_cluster_snapshots = ['gp-database-cluster','dv-postgres-repository-unencrypted','dv-postgres-repository-unencrypted']
    # encrypted_snapshots = ['bxrealtimeanalyticsstack-snapshot-bd1cp3fboi9g82i-8ne3ao4gep2u-encrypted','bxrealtimeanalyticsstack-snapshot-databaseb269d8bb-jv67p9irdzez-encrypted','bxrealtimeanalyticsstack-snapshot-databaseb269d8bb-od1l7qlazk2a-encrypted','bxrealtimeanalyticsstack-snapshot-databasebxrealtimeanalyticsstack6efb3388-11gobb7dy5jrj-encrypted','bxrealtimeanalyticsstack-snapshot-databasebxrealtimeanalyticsstack6efb3388-1238i48byub5a-encrypted','bxrealtimeanalyticsstack-snapshot-databasebxrealtimeanalyticsstack6efb3388-1tdq22fnu22n6-encrypted','bxrtanalyticsstack-snapshot-analytics-rt-db-yi8c565raz6l-encrypted','bxrtanalyticsstack-snapshot-bd17pyoi87zwk6k-l9mdqldax74u-encrypted','bxrtanalyticsstack-snapshot-bd1q9evsnp58waz-pflbr3bwdzq9-encrypted','bxrtanalyticsstack-snapshot-bdbjbrs7l1efye-rx694o73p0lw-encrypted','bxrtanalyticsstack-snapshot-bdi92yaiq0g8dj-16bcq3rcsogaf-encrypted','bxrtanalyticsstack-snapshot-bdoayblv8cj4cr-8t9xllc95nk2-encrypted','bxrtanalyticsstack-snapshot-bdps8up0m0ohfa-1qe1ywwbofgws-encrypted','bxrtanalyticsstack-snapshot-bdtwbr4qjdve9c-1b9qxbuqc0jtw-encrypted']
    # encrypted_snapshots = ['bxrtanalyticsstack-snapshot-bdy8xee4tqr6c0-k8ous4hk0ny4-encrypted','bxrtanalyticsstack-snapshot-databasebxrtanalyticsstack1989081d-115zma9rk38hs-encrypted','bxrtanalyticsstack-snapshot-databasebxrtanalyticsstack1989081d-1ismbltjimws1-encrypted','bxrtanalyticsstack-snapshot-databasebxrtanalyticsstack1989081d-1kzq5rdcdnm3w-encrypted','bxrtanalyticsstack-snapshot-databasebxrtanalyticsstack1989081d-66xj4knza3pg-encrypted','bxrtanalyticsstack-snapshot-databasebxrtanalyticsstack1989081d-pm68cs31zxz2-encrypted','bxrtanalyticsstack-snapshot-databasebxrtanalyticsstack1989081d-q7o75avygvbj-encrypted','canary-bxrealtimeanalyticsstack-snapshot-databaseb269d8bb-gva7kglzayk6-encrypted','database-manos-flyway-flywaytestid1-final-snapshot-encrypted','database-manos-flyway-flywaytestid1-final-snapshot-dnd-encrypted','db-dump-encrypted','emr-5-adhoc-dev-eu-west-1-11-8-jan-25-encrypted','emr-5-adhoc-dev-eu-west-1-9-5-jan-25-encrypted','emr-5-adhoc-dev-eu-west-1-9-6-jan-25-encrypted']
    #encrypted_snapshots = ['emr-5-adhoc-dev-eu-west-1-final-snapshot-encrypted','emr-5-adhoc-dev-eu-west-1-final-snapshot-dnd-encrypted','emr-5-batch-dev-eu-west-1-11-8-jan-25-encrypted','emr-5-batch-dev-eu-west-1-9-6-jan-25-encrypted','emr-5-batch-dev-eu-west-1-final-snapshot-encrypted','emr-5-batch-dev-eu-west-1-v9-5-19-preupgrade-encrypted','emr-5-batch-dev-eu-west-1-v9-6-18-preupgrade-encrypted','emr-5-batch-flows-dev-eu-west-1-11-8-jan-25-encrypted','emr-5-batch-flows-dev-eu-west-1-9-6-18-jan-25-pre-ug-encrypted','emr-5-batch-flows-dev-eu-west-1-final-snapshot-encrypted','emr-5-batch-flows-dev-eu-west-1-v9-5-19-preupgrade-encrypted','emr-5-batch-flows-dev-eu-west-1-v9-6-18-preupgrade-encrypted','emr-5-cruncher-dev-eu-west-1-final-snapshot-encrypted','emr-5-import-dev-eu-west-1-11-8-jan-25-encrypted']
    #encrypted_snapshots = ['emr-5-adhoc-dev-eu-west-1-final-snapshot-encrypted','emr-5-adhoc-dev-eu-west-1-final-snapshot-dnd-encrypted','emr-5-batch-dev-eu-west-1-11-8-jan-25-encrypted','emr-5-batch-dev-eu-west-1-9-6-jan-25-encrypted','emr-5-batch-dev-eu-west-1-final-snapshot-encrypted','emr-5-batch-dev-eu-west-1-v9-5-19-preupgrade-encrypted','emr-5-batch-dev-eu-west-1-v9-6-18-preupgrade-encrypted','emr-5-batch-flows-dev-eu-west-1-11-8-jan-25-encrypted','emr-5-batch-flows-dev-eu-west-1-9-6-18-jan-25-pre-ug-encrypted','emr-5-batch-flows-dev-eu-west-1-final-snapshot-encrypted','emr-5-batch-flows-dev-eu-west-1-v9-5-19-preupgrade-encrypted','emr-5-batch-flows-dev-eu-west-1-v9-6-18-preupgrade-encrypted','emr-5-cruncher-dev-eu-west-1-final-snapshot-encrypted','emr-5-import-dev-eu-west-1-11-8-jan-25-encrypted']
    #encrypted_snapshots = ['flywaytestid-snapshot-databaseflywaytestid18c0b960-1ar0omkyzh3st-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-1cjvefoghv7vc-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-1dwjw1n95t6sh-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-1h2o5zavq4pgl-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-1xrza76h5ithg-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-8vx4ozchudfa-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-c1rioambz0kp-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-daq9sjdw4fwd-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-fnbn1kodcdwb-encrypted']
    #encrypted_snapshots = ['flywaytestid-snapshot-databaseflywaytestid18c0b960-lyfqk7dirvlx-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-p56nce8vfd64-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-qbpszmv2243m-encrypted','hue-eu-west-1-dev-final-snapshot-encrypted','hue-eu-west-1-dev-final-snapshot-dnd-encrypted','looked-db-eu-west-1-dev-202303091526-encrypted','looker-6-db-eu-west-1-dev-final-snapshot-dnd-encrypted','looker-db-eu-west-1-dev-classic-final-snapshot-encrypted','looker-db-eu-west-1-dev-dec-2018-final20210811-encrypted']
    #encrypted_snapshots = ['looker-db-eu-west-1-dev-dec-2018-final-snapshot-encrypted','looker-db-staging-eu-west-1-final-snapshot-20141013-final-snapshot-encrypted','looker-deb-eu-west-1-dev-vpc-encrypted','looker-shrink-disk-encrypted','pre-62-encrypted','preencryptionsnapshot-encrypted','sonar-classic-final-snapshot-encrypted','sonar-dev-vpc-encrypted','sonar-final-20210811-encrypted']
    #encrypted_snapshots = ['sonar-final-snapshot-encrypted','sonarqube1-final-snapshot-encrypted','sonarqube1-final-snapshot-dnd-encrypted','sonar-vpc-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-6k517lclyi2e-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-frged7bpijbd-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-haz6lclimacq-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-hfcrikmbwn75-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-qpuxl4nuhvud-encrypted']
    encrypted_snapshots = ['flywaytestid4-snapshot-databaseflywaytestid41b432d9e-8db296t5m43b-encrypted','flywaytestid3-snapshot-databaseflywaytestid3896112be-yywagfab2bgg-encrypted','flywaytestid-snapshot-databaseflywaytestid18c0b960-16mrknt6evqc2-encrypted','flywaytestid4-snapshot-databaseflywaytestid41b432d9e-7ciusv4g3uom-encrypted','flywaytestid5-snapshot-databaseflywaytestid5bbd7642f-1el6ql46d32mg-encrypted','flywaytestid4-snapshot-databaseflywaytestid41b432d9e-8db296t5m43b-encrypted','flywaytestid3-snapshot-databaseflywaytestid3896112be-yywagfab2bgg-encrypted','flywaytestid5-snapshot-databaseflywaytestid5bbd7642f-1el6ql46d32mg-encrypted','flywaytestid4-snapshot-databaseflywaytestid41b432d9e-7ciusv4g3uom-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-rm1sixiu7klh-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-rsmxc7rfmjg4-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-twlpoerdxuyf-encrypted','testbxrealtimeanalyticsstack-snapshot-databaseb269d8bb-xjdhbdjjdnc3-encrypted','testrtastack-snapshot-databaseb269d8bb-0zp84kpf7wnt-encrypted','testrtastack-snapshot-databaseb269d8bb-27nj7ffayy3u-encrypted','testrtastack-snapshot-databaseb269d8bb-2vpglv0rp6fb-encrypted','testrtastack-snapshot-databaseb269d8bb-67zj3kal91xl-encrypted','testrtastack-snapshot-databaseb269d8bb-9hclkzgqarok-encrypted']


    
    tasks = []
    # for snapshot in unencrypted_rds_db_snapshots:
    for snapshot in encrypted_snapshots:
        # snapshot_id = snapshot
        # encrypted_snapshot_id = await create_encrypted_db_copy(snapshot_id)
        snapshot_id = snapshot.removesuffix('-encrypted')
        encrypted_snapshot_id =  snapshot
        tasks.append(delete_unencrypted_db_snapshot(snapshot_id, encrypted_snapshot_id))

    # for snapshot in unencrypted_rds_cluster_snapshots:
    #     snapshot_id = snapshot
    #     encrypted_snapshot_id = await create_encrypted_cluster_copy(snapshot_id)
    #     tasks.append(delete_unencrypted_cluster_snapshot(snapshot_id, encrypted_snapshot_id))
        

    await asyncio.gather(*tasks)

asyncio.run(main())






from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook, AwsBaseHook
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from dag_content.tower_metrics_content import *


@dag(
    schedule_interval="@weekly",
    start_date=datetime(2022, 11, 11),
    catchup=False,
    default_args={
        "retries": 3,
    },
    tags=["Nextflow Tower Metrics"],
)
def conn_testing_dag():
    @task()
    def get_db_cluster_info():
        hook_kwargs = {"aws_conn_id": "TOWER_DB_CONNECTION", "region_name": "us-east-1"}
        rds = RdsHook(**hook_kwargs)
        state = rds.get_db_cluster_state("tower")
        rds_client = rds.get_conn()
        info = rds_client.describe_db_clusters(
        DBClusterIdentifier="tower",
        )
        print("=======================================")
        print(state)
        print("=======================================")

    @task()
    def create_rds_data_session():
        rds_data = AwsBaseHook(aws_conn_id="TOWER_DB_CONNECTION", client_type="rds-data", region_name="us-east-1")
        rds_data_client = rds_data.get_conn()
        print("=======================================")
        print(rds_data_client)
        print("=======================================")
    # # get needed info from production database for cloining
    # prod_db_info = get_database_info(aws_creds=AWS_CREDS, db_name=DATABASE_NAME)
    # # clone tower database
    # clone_db_info = clone_tower_database(
    #     aws_creds=AWS_CREDS,
    #     db_name=DATABASE_NAME,
    #     clone_name=CLONE_DATABASE_NAME,
    #     subnet_group=prod_db_info["subnet_group"],
    #     security_group=prod_db_info["security_group"],
    # )
    # # generate new password while clone is spinning up
    # password = generate_random_password(aws_creds=AWS_CREDS)
    # # update cloned database to have new password
    # modify = modify_database_clone(
    #     aws_creds=AWS_CREDS, clone_name=CLONE_DATABASE_NAME, password=password
    # )
    # # update secret with new password and cloned database info while it is being modified
    # secret_arn = update_secret(
    #     aws_creds=AWS_CREDS,
    #     clone_name=CLONE_DATABASE_NAME,
    #     db_info=clone_db_info,
    #     password=password,
    # )
    # # create json-friendly list of dicts with query results for reporting
    # json_list = query_database(
    #     aws_creds=AWS_CREDS,
    #     resource_arn=clone_db_info["resource_arn"],
    #     secret_arn=secret_arn,
    # )
    # # export json report to synapse
    # export = export_json_to_synapse(json_list=json_list)
    # # notify interested parties of the new report
    # send = send_synapse_notification()
    # # delete cloned database - wait for it to be gone before completing process
    # delete = delete_clone_database(aws_creds=AWS_CREDS, clone_name=CLONE_DATABASE_NAME)


    # #add missing dependencies
    # prod_db_info >> clone_db_info
    # [clone_db_info, password] >> modify
    # [clone_db_info, password] >> secret_arn
    # [secret_arn, modify] >> json_list
    # json_list >> [export, delete]
    # export >> send
    get_db_cluster_info()
    create_rds_data_session()

conn_testing_dag = conn_testing_dag()

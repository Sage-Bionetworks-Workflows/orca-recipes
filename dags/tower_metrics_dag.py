from datetime import datetime

from airflow.decorators import dag

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
def tower_metrics_dag():
    # get needed info from production database for cloining
    prod_db_info = get_database_info(aws_creds=AWS_CREDS, db_name=DATABASE_NAME)
    # clone production database - wait until it is completes to move on to modification
    clone_db_info = clone_tower_database(
        aws_creds=AWS_CREDS,
        db_name=DATABASE_NAME,
        clone_name=CLONE_DATABASE_NAME,
        subnet_group=prod_db_info["subnet_group"],
        security_group=prod_db_info["security_group"],
    )
    # generate new password while clone is spinning up
    password = generate_random_password(aws_creds=AWS_CREDS)
    # update cloned database to have new password - wait until modification has completed to move on to querying
    modify = modify_cloned_cluster(
        aws_creds=AWS_CREDS, clone_name=CLONE_DATABASE_NAME, password=password
    )
    # update secret with new password and cloned database info while it is being modified
    secret_arn = update_secret(
        aws_creds=AWS_CREDS,
        clone_name=CLONE_DATABASE_NAME,
        db_info=clone_db_info,
        password=password,
    )
    # create json-friendly list of dicts with query results for reporting
    json_list = query_database(
        aws_creds=AWS_CREDS,
        resource_arn=clone_db_info["resource_arn"],
        secret_arn=secret_arn,
    )
    # export json report to synapse
    export = export_json_to_synapse(json_list=json_list)
    # notify interested parties of the new report
    send = send_synapse_notification()
    # delete cloned database - wait for it to be gone before completing process
    delete = delete_clone_database(aws_creds=AWS_CREDS, clone_name=CLONE_DATABASE_NAME)


    #add missing dependencies
    prod_db_info >> clone_db_info
    [clone_db_info, password] >> modify
    [clone_db_info, password] >> secret_arn
    [secret_arn, modify] >> json_list
    json_list >> [export, delete]
    export >> send

tower_metrics_dag = tower_metrics_dag()

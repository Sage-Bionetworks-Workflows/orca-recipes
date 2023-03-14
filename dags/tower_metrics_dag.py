from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from dag_content.tower_metrics_content import *

dag_params = {
    "user_list": Param(
        "bwmac,thomas.yu",  # no support for python lists
        type="string",
    )
}


@dag(
    schedule_interval="0 15 * * 5",  # Fridays at 3PM PST
    start_date=datetime(2022, 11, 11),
    catchup=False,
    default_args={
        "retries": 3,
    },
    tags=["Nextflow Tower Metrics"],
    params=dag_params,
)
def tower_metrics_dag():
    # get needed info from production database for cloining
    prod_db_info = get_database_info(db_name=DATABASE_NAME)
    # clone tower database
    clone_db_info = clone_tower_database(
        db_name=DATABASE_NAME,
        clone_name=CLONE_DATABASE_NAME,
        subnet_group=prod_db_info["subnet_group"],
        security_group=prod_db_info["security_group"],
    )
    # generate new password while clone is spinning up
    password = generate_random_password()
    # update cloned database to have new password
    modify = modify_database_clone(clone_name=CLONE_DATABASE_NAME, password=password)
    # update secret with new password and cloned database info while it is being modified
    secret_arn = update_secret(
        clone_name=CLONE_DATABASE_NAME,
        db_info=clone_db_info,
        password=password,
    )
    # create json-friendly list of dicts with query results for reporting
    json_list = query_database(
        resource_arn=clone_db_info["resource_arn"],
        secret_arn=secret_arn,
    )
    # export json report to synapse
    export = export_json_to_synapse(json_list=json_list)
    # notify interested parties of the new report
    send = send_synapse_notification()
    # delete cloned database - wait for it to be gone before completing process
    delete = delete_clone_database(clone_name=CLONE_DATABASE_NAME)

    # add missing dependencies
    prod_db_info >> clone_db_info
    [clone_db_info, password] >> modify
    [clone_db_info, password] >> secret_arn
    [secret_arn, modify] >> json_list
    json_list >> [export, delete]
    export >> send


tower_metrics_dag = tower_metrics_dag()

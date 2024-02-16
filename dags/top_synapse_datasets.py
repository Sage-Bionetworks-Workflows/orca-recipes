from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook



dag_params = {
    "snowflake_conn_id": Param(
        "SNOWFLAKE_SYSADMIN_SYNAPSE_DATA_WAREHOUSE_CONN", type="string"
    ),
    "slack_conn_id": Param(
        "SLACK_TOPCHARTBOT_CONN", type="string"
    )
}

dag_config = {
    "schedule_interval": "0 0 * * 1",
    "start_date": datetime(2024, 1, 10),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params
}


@dag(**dag_config)
def top_charts():
    @task
    def get_top_charts(**context):
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        query = """
        WITH DEDUP_FILEHANDLE AS (
            SELECT DISTINCT
                FILEDOWNLOAD.USER_ID,
                FILEDOWNLOAD.FILE_HANDLE_ID AS FD_FILE_HANDLE_ID,
                FILEDOWNLOAD.RECORD_DATE,
                FILEDOWNLOAD.PROJECT_ID,
                file_latest.content_size
            FROM
                SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILEDOWNLOAD
            inner join
                synapse_data_warehouse.synapse.file_latest
            on
                FILEDOWNLOAD.file_handle_id = file_latest.id
            where
                record_date > DATEADD(HOUR, -24, CURRENT_DATE)
        ),

        PUBLIC_PROJECTS AS (
            SELECT
                NODE_LATEST.PROJECT_ID
                -- sum(file_latest.content_size) as project_size,
                -- array_agg(distinct file_latest.bucket) as buckets
            FROM
                SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST
            join
                synapse_data_warehouse.synapse.file_latest
            on
                node_latest.file_handle_id = file_latest.id
            WHERE
                node_latest.IS_PUBLIC and
                project_id in (SELECT distinct project_id from SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST where is_public and node_type = 'project')
            group by
                node_latest.project_id

        ),
        download_stat as (
            SELECT
                project_id,
                count(RECORD_DATE) AS DOWNLOADS_PER_PROJECT,
                count(DISTINCT USER_ID) AS NUMBER_OF_UNIQUE_USERS_DOWNLOADED,
                count(DISTINCT FD_FILE_HANDLE_ID) AS NUMBER_OF_UNIQUE_FILES_DOWNLOADED
                -- sum(DEDUP_FILEHANDLE.content_size) / power(2, 40) as data_download_size_in_tebibytes
            FROM
                DEDUP_FILEHANDLE
            where
                project_id IN (SELECT PROJECT_ID FROM PUBLIC_PROJECTS)
            GROUP BY
                DEDUP_FILEHANDLE.PROJECT_ID
        )
        select
            'https://www.synapse.org/#!Synapse:syn' || cast(download_stat.project_id as varchar) as project,
            node_latest.name,
            download_stat.DOWNLOADS_PER_PROJECT
            -- download_stat.data_download_size_in_tebibytes,
            -- download_stat.NUMBER_OF_UNIQUE_USERS_DOWNLOADED,
            -- download_stat.NUMBER_OF_UNIQUE_FILES_DOWNLOADED
        from
            download_stat
        inner join
            public_projects
        on
            download_stat.project_id = public_projects.project_id
        left join
            synapse_data_warehouse.synapse.node_latest
        on
            download_stat.project_id = node_latest.id
        order by
            DOWNLOADS_PER_PROJECT DESC NULLS LAST
        limit 5;
        """
        snow_hook.run(query)
    
    @task
    def message_to_slack(message, **context):
        hook = SlackWebhookHook(slack_webhook_conn_id=context["params"]["slack_conn_id"])
        hook.send_dict({"text": message})


    message = get_top_charts()
    load = message_to_slack(message=message)

    message >> load


top_charts()

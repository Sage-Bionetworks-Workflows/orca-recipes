"""This script is used to execute a query on Snowflake and report the results to a
slack channel. See ORCA-301 for more context."""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from slack_sdk import WebClient


dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_SYSADMIN_PORTAL_RAW_CONN", type="string"),
}

dag_config = {
    "schedule_interval": "0 18 * * *",
    "start_date": datetime(2024, 2, 20),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}

QUERY = """
WITH PUBLIC_PROJECTS AS (
    SELECT
        node_latest.project_id,
        node_latest.name
    FROM
        synapse_data_warehouse.synapse.node_latest
    WHERE
        node_latest.is_public AND
        node_latest.node_type = 'project'
),
DEDUP_FILEHANDLE AS (
    SELECT DISTINCT
        PUBLIC_PROJECTS.name,
        filedownload.user_id,
        filedownload.file_handle_id AS FD_FILE_HANDLE_ID,
        filedownload.record_date,
        filedownload.project_id,
        file_latest.content_size
    FROM
        synapse_data_warehouse.synapse.filedownload
    INNER JOIN
        PUBLIC_PROJECTS
    ON
        filedownload.project_id = PUBLIC_PROJECTS.project_id
    INNER JOIN
        synapse_data_warehouse.synapse.file_latest
    ON
        filedownload.file_handle_id = file_latest.id
    WHERE
        filedownload.record_date > DATEADD(HOUR, -24, CURRENT_DATE)
),

DOWNLOAD_STAT AS (
    SELECT
        name,
        project_id,
        count(record_date) AS DOWNLOADS_PER_PROJECT,
        count(DISTINCT user_id) AS NUMBER_OF_UNIQUE_USERS_DOWNLOADED,
        count(DISTINCT FD_FILE_HANDLE_ID) AS NUMBER_OF_UNIQUE_FILES_DOWNLOADED
    FROM
        DEDUP_FILEHANDLE
    GROUP BY
        project_id, name
)
SELECT
    'https://www.synapse.org/#!Synapse:syn' || cast(DOWNLOAD_STAT.project_id as varchar) as project,
    DOWNLOAD_STAT.name,
    DOWNLOAD_STAT.DOWNLOADS_PER_PROJECT
FROM
    DOWNLOAD_STAT
ORDER BY
    DOWNLOADS_PER_PROJECT DESC NULLS LAST
LIMIT 10;
"""


@dag(**dag_config)
def snowflake_top_downloads_to_slack():
    @task
    def get_top_downloads_from_snowflake(**context):
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        cs.execute(QUERY)
        top_downloaded_df = cs.fetch_pandas_all()

        message = ":synapse: Top Downloaded Public Synapse Projects Yesterday!\n\n"
        for index, row in top_downloaded_df.iterrows():
            name = row["NAME"]
            url = row["PROJECT"]
            downloads = row["DOWNLOADS_PER_PROJECT"]
            message += f"{index+1}. <{url}|{name}> - {downloads} downloads\n\n"
        message += "One download is a user downloading a file once\n"
        return message

    @task
    def post_top_downloads_to_slack(message: str):
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        result = client.chat_postMessage(channel="topcharts", text=message)
        return result

    top_downloads = get_top_downloads_from_snowflake()
    post_to_slack = post_top_downloads_to_slack(message=top_downloads)

    top_downloads >> post_to_slack


snowflake_top_downloads_to_slack()

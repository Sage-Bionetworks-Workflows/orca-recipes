"""This DAG executes a query on Snowflake retrieving the top X most downloaded publically abailable Synapse projects from the day prior
to the provided date (defaults to today's date) and report the results to a Slack channel and Synapse table. 
See ORCA-301 for more context.

A Note on the `backfill` functionality:
In addition to the fact that this DAG pulls data from the day prior to the provided date, there is an extra wrinkle when using the `backfill` functionality.
In the Synapse table UI, data is displayed at the local datetime of the user, but the Snowflake query is performed at UTC time. So, if we take into account both of these time differences
for someone living in North America, you will need to provide a `backfill_date` 2 days after the date that the data is missing in the Synapse table. For example,
if data is missing for "2025-01-03", `backfill_date` will need to be set to "2025-01-05". Additionally, be sure to set `backfill` to `true` or the DAG will run normally and post
the results to Slack.

DAG Parameters:
- `snowflake_developer_service_conn`: A JSON-formatted string containing the connection details required to authenticate and connect to Snowflake.
- `synapse_conn_id`: The connection ID for the Synapse connection.
- `hours_time_delta`: The number of hours to subtract from the current date to get the date for the query. Defaults to `24`.
- `backfill`: Whether to backfill the data. Defaults to `False`.
- `backfill_date`: The date to backfill the data from. Will be ignored if `backfill` is `False`.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook
from slack_sdk import WebClient

dag_params = {
    "snowflake_developer_service_conn": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    # hours_time_delta is the number of hours to subtract from the current date to get the date for the query
    "hours_time_delta": Param("24", type="string"),
    "backfill": Param(False, type="boolean"),
    # backfill_date string format: YYYY-MM-DD
    "backfill_date": Param("1900-01-01", type="string"),
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

SIZE_ROUNDING = 3
BYTE_STRING = "GiB"
# 30 is the power of 2 for GiB, 40 is the power of 2 for TiB
POWER_OF_TWO = 30

SYNAPSE_RESULTS_TABLE = "syn53696951"
SYNAPSE_HOMEPAGE_PROJECT_ID = 23593546


@dataclass
class DownloadMetric:
    """Dataclass to hold the download metrics from Synapse.

    Attributes:
        name: The name of the project
        project: The ID of the project
        downloads_per_project: The number of downloads per project
        number_of_unique_users_downloaded: The number of unique users who downloaded
        data_download_size: The size of the data downloaded

    """

    name: str
    project: str
    downloads_per_project: int
    number_of_unique_users_downloaded: int
    data_download_size: float


@dag(**dag_config)
def top_public_synapse_projects_from_snowflake() -> None:
    """Execute a query on Snowflake and report the results to a slack channel and
    synapse table."""

    @task
    def get_top_downloads_from_snowflake(**context) -> List[DownloadMetric]:
        """Execute the query on Snowflake and return the results."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()

        # set backfill_date to None if backfill is False
        backfill_date = context["params"]["backfill_date"]
        query_date = None if not context["params"]["backfill"] else f"'{backfill_date}'"

        query = f"""
            WITH PUBLIC_PROJECTS AS (
                SELECT
                    node_latest.project_id,
                    node_latest.name
                FROM
                    synapse_data_warehouse.synapse.node_latest
                WHERE
                    node_latest.is_public AND
                    node_latest.node_type = 'project' AND
                    node_latest.project_id != {SYNAPSE_HOMEPAGE_PROJECT_ID}
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
                    filedownload.record_date = DATEADD(HOUR, -{context["params"]["hours_time_delta"]}, {query_date or "CURRENT_DATE"})
            ),

            DOWNLOAD_STAT AS (
                SELECT
                    name,
                    project_id,
                    count(record_date) AS DOWNLOADS_PER_PROJECT,
                    count(DISTINCT user_id) AS NUMBER_OF_UNIQUE_USERS_DOWNLOADED,
                    count(DISTINCT FD_FILE_HANDLE_ID) AS NUMBER_OF_UNIQUE_FILES_DOWNLOADED,
                    sum(content_size) as data_download_size
                FROM
                    DEDUP_FILEHANDLE
                GROUP BY
                    project_id, name
            )
            SELECT
                'syn' || cast(DOWNLOAD_STAT.project_id as varchar) as project,
                DOWNLOAD_STAT.name,
                DOWNLOAD_STAT.DOWNLOADS_PER_PROJECT,
                DOWNLOAD_STAT.data_download_size,
                DOWNLOAD_STAT.NUMBER_OF_UNIQUE_USERS_DOWNLOADED
            FROM
                DOWNLOAD_STAT
            ORDER BY
                NUMBER_OF_UNIQUE_USERS_DOWNLOADED DESC NULLS LAST
            """
        cs.execute(query)
        top_downloaded_df = cs.fetch_pandas_all()

        metrics = []
        for _, row in top_downloaded_df.iterrows():
            metrics.append(
                DownloadMetric(
                    name=row["NAME"],
                    project=row["PROJECT"],
                    downloads_per_project=row["DOWNLOADS_PER_PROJECT"],
                    number_of_unique_users_downloaded=row[
                        "NUMBER_OF_UNIQUE_USERS_DOWNLOADED"
                    ],
                    data_download_size=row["DATA_DOWNLOAD_SIZE"] or 0,
                )
            )
        return metrics

    @task.branch()
    def check_backfill(**context) -> str:
        """Check if the backfill is enabled. When it is, do not post to Slack."""
        if context["params"]["backfill"]:
            return "stop_dag"
        return "generate_top_downloads_message"

    @task()
    def stop_dag() -> None:
        """Stop the DAG."""
        pass

    @task
    def generate_top_downloads_message(metrics: List[DownloadMetric], **context) -> str:
        """Generate the message to be posted to the slack channel."""
        message = f":synapse: Top Downloaded Public Synapse Projects {'Yesterday' if int(context['params']['hours_time_delta']) <= 24 else (date.today() - timedelta(hours=int(context['params']['hours_time_delta']))).isoformat()}!\n\n"
        for index, row in enumerate(metrics[:10]):
            if row.data_download_size:
                size_string = f"{(row.data_download_size / 2 ** POWER_OF_TWO):.{SIZE_ROUNDING}f} {BYTE_STRING}"
            else:
                size_string = f"< {0:.{SIZE_ROUNDING}f}5 {BYTE_STRING}"
            message += f"{index+1}. <https://www.synapse.org/#!Synapse:{row.project}|{row.name}> - {row.downloads_per_project} downloads, {row.number_of_unique_users_downloaded} unique users, {size_string} egressed\n\n"
        message += "One download is a user downloading an entity (File, Table, Views, etc) once\n"
        return message

    @task
    def post_top_downloads_to_slack(message: str) -> bool:
        """Post the top downloads to the slack channel."""
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        result = client.chat_postMessage(channel="topcharts", text=message)
        print(f"Result of posting to slack: [{result}]")
        return result is not None

    @task
    def push_results_to_synapse_table(metrics: List[DownloadMetric], **context) -> None:
        """Push the results to a Synapse table."""
        data = []
        # convert context["params"]["backfill_date"] to date in same format as date.today()
        today = (
            date.today()
            if not context["params"]["backfill"]
            else datetime.strptime(
                context["params"]["backfill_date"], "%Y-%m-%d"
            ).date()
        )
        yesterday = today - timedelta(hours=int(context["params"]["hours_time_delta"]))
        for metric in metrics:
            data.append(
                [
                    metric.project,
                    metric.downloads_per_project,
                    yesterday,
                    metric.number_of_unique_users_downloaded,
                    metric.data_download_size,
                ]
            )
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=SYNAPSE_RESULTS_TABLE, values=data)
        )

    top_downloads = get_top_downloads_from_snowflake()
    check = check_backfill()
    stop = stop_dag()
    slack_message = generate_top_downloads_message(metrics=top_downloads)
    post_to_slack = post_top_downloads_to_slack(message=slack_message)
    push_to_synapse_table = push_results_to_synapse_table(metrics=top_downloads)

    top_downloads >> check >> [stop, slack_message]
    slack_message >> post_to_slack
    top_downloads >> push_to_synapse_table


top_public_synapse_projects_from_snowflake()

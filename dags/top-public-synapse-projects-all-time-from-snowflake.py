"""This script is used to execute a query on Snowflake and report the results to a
slack channel and Synapse table. 
This retrieves the top X publicly downloaded Synapse projects for slack.
This retrieves all publicly downloaded Synapse projects for the Synapse table.
See ORCA-301 for more context."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_SYSADMIN_PORTAL_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
}

dag_config = {
    # Run at 00:00 on the first day of the month
    "schedule_interval": "0 0 1 * *",
    "start_date": datetime(2024, 4, 1),
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

SYNAPSE_RESULTS_TABLE = "syn55259224"


@dataclass
class DownloadMetric:
    """Dataclass to hold the download metrics from Synapse.

    Attributes:
        name: The name of the project
        project: The ID of the project
        downloads_per_project: The number of downloads per project
        number_of_unique_users_downloaded: The number of unique users who downloaded
        data_download_size: The size of the data downloaded
        export_date: The a timestamp when the data was exported

    """

    name: str
    project: str
    downloads_per_project: int
    number_of_unique_users_downloaded: int
    data_download_size: float
    export_date: datetime


@dag(**dag_config)
def top_public_synapse_projects_all_time_from_snowflake() -> None:
    """Execute a query on Snowflake and report the results to a slack channel and
    synapse table."""

    @task
    def get_all_time_downloads_from_snowflake(**context) -> List[DownloadMetric]:
        """Execute the query on Snowflake and return the results."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        query = f"""
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
            -- joion all file downloads that occurred yesterday
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
                -- remove this for all time?
                -- WHERE
                --     filedownload.record_date = DATEADD(HOUR, -{context["params"]["hours_time_delta"]}, CURRENT_DATE)
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
                DOWNLOAD_STAT.NUMBER_OF_UNIQUE_USERS_DOWNLOADED,
                CURRENT_TIMESTAMP as EXPORT_DATE
            FROM
                DOWNLOAD_STAT
            ORDER BY
                DOWNLOADS_PER_PROJECT DESC NULLS LAST;
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
                    export_date=row["EXPORT_DATE"],
                )
            )
        return metrics


    @task
    def push_results_to_synapse_table(metrics: List[DownloadMetric], **context) -> None:
        """Push the results to a Synapse table."""
        data = []
        for metric in metrics:
            data.append(
                [
                    metric.project,
                    metric.downloads_per_project,
                    metric.number_of_unique_users_downloaded,
                    metric.data_download_size,
                    metric.export_date,
                ]
            )

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=SYNAPSE_RESULTS_TABLE, values=data)
        )

    top_downloads = get_all_time_downloads_from_snowflake()
    push_to_synapse_table = push_results_to_synapse_table(metrics=top_downloads)

    top_downloads >> push_to_synapse_table


top_public_synapse_projects_all_time_from_snowflake()

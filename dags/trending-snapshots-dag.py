"""
This script is used to execute a query on Snowflake and report the results to a 
Synapse table. This DAG updates the Trending Snapshots Synapse table
(https://www.synapse.org/Synapse:syn61597055/tables/) every month with the following metrics:

- Top 10 Projects with the most unique users
- Number of unique users
- Last download date
- Total data size (in GiB) for each project
- Export date (the last time this table was updated)
"""

from airflow.decorators import dag

from dataclasses import dataclass
from datetime import date, datetime
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook


SYNAPSE_RESULTS_TABLE = "syn61932294"

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_SYSADMIN_PORTAL_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "current_date": Param(date.today().strftime("%Y-%m-%d"), type="string"),
    "time_window": Param("30d", type="string"),
    }

dag_config = {
    # run on the 1st of every month at midnight
    "schedule_interval": "0 0 1 * *",
    "start_date": datetime(2024, 7, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}


@dataclass
class SnapshotsMetric:
    """
    Dataclass to hold the download metrics from Synapse.

    Attributes:
        total_data_size_in_pib: The size of the data hosted on Synapse in PiB
        active_users_last_month: The number of active users last month
        total_downloads_last_month: The total number of downloads by users last month
    """

    project_id: float
    n_unique_users: int
    last_download_date: int
    total_data_size_in_gib: float


@dag(**dag_config)
def trending_snapshots() -> None:
    """
    """

    @task
    def get_trending_snapshots(**context) -> List[SnapshotsMetric]:
        """Execute the query on Snowflake and return the results."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        query = f"""
                WITH RECENT_DOWNLOADS AS (
                    SELECT *
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILEDOWNLOAD
                    WHERE 1=1
                    AND RECORD_DATE > DATEADD(DAY, -28, '2024-06-26') 
                    AND RECORD_DATE <= '2024-06-26'
                    AND STACK = 'prod'
                    AND PROJECT_ID IS NOT NULL
                ),
                PUBLIC_PROJECTS AS (
                    SELECT NAME, PROJECT_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST
                    WHERE 1=1
                    AND NODE_TYPE = 'project'
                    AND IS_PUBLIC = TRUE
                )

                SELECT 
                    recent_downloads.PROJECT_ID, 
                    COUNT(DISTINCT recent_downloads.USER_ID) AS unique_users,
                    MAX(recent_downloads.RECORD_DATE) AS last_download_date,
                    SUM(file_latest.CONTENT_SIZE) / POWER(1024, 3) AS TOTAL_DATA_SIZE_IN_GIB

                -- Select the table you want to curate from
                FROM 
                    RECENT_DOWNLOADS recent_downloads

                -- Join FILE_LATEST
                JOIN 
                    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST file_latest
                ON 
                    recent_downloads.FILE_HANDLE_ID = file_latest.ID

                -- Join PUBLIC_PROJECTS
                JOIN
                    PUBLIC_PROJECTS public_projects
                ON
                    recent_downloads.PROJECT_ID = public_projects.PROJECT_ID

                -- Sorting the final table for calculation
                GROUP BY
                    recent_downloads.PROJECT_ID
                ORDER BY 
                    unique_users DESC
                LIMIT 10;
            """

        cs.execute(query)
        top_downloaded_df = cs.fetch_pandas_all()

        metrics = []
        for _, row in top_downloaded_df.iterrows():
            metrics.append(
                SnapshotsMetric(
                    project_id=row["project_id"],
                    n_unique_users=row["n_unique_users"],
                    last_download_date=row["last_download_date"],
                    total_data_size_in_gib=row["total_data_size_in_gib"],
                )
            )
        return metrics

    @task
    def push_results_to_synapse_table(metrics: List[SnapshotsMetric], **context) -> None:
        """Push the results to a Synapse table."""
        data = []
        today = date.today()
        for metric in metrics:
            data.append(
                [
                    metric.project_id,
                    metric.n_unique_users,
                    metric.last_download_date,
                    metric.total_data_size_in_gib,
                    today
                ]
            )

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=SYNAPSE_RESULTS_TABLE, values=data)
        )

    top_downloads = get_trending_snapshots()
    push_to_synapse_table = push_results_to_synapse_table(metrics=top_downloads)

    top_downloads >> push_to_synapse_table


trending_snapshots()

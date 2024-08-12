"""
This script is used to execute a query on Snowflake and report the results to a 
Synapse table. This DAG updates the Trending Projects Snapshots Synapse table
(https://www.synapse.org/Synapse:syn61597055/tables/) every X number of days with the following metrics:

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
from dateutil.relativedelta import relativedelta

import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook


SYNAPSE_RESULTS_TABLE = "syn61597055"

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_SYSADMIN_PORTAL_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "current_date": Param(date.today().strftime("%Y-%m-%d"), type="string"),
    "month_to_run": Param((date.today() - relativedelta(months=1)).strftime("%Y-%m-%d"), type="string")
    }

dag_config = {
    # run on the 2nd of every month at midnight
    "schedule_interval": "0 0 2 * *",
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}


@dataclass
class SnapshotMetrics:
    """
    Dataclass to hold the download metrics from Synapse.

    Attributes:
        project_id: The ID of the Project
        n_unique_users: The number of unique users who downloaded from the Project
        last_download_date: The date of the last download from the Project
        total_data_size_in_gib: The current total size of the Project in GiB
    """

    project_id: float
    n_unique_users: int
    last_download_date: int
    total_data_size_in_gib: float


@dag(**dag_config)
def trending_projects_snapshot() -> None:
    """
    This DAG executes a query on Snowflake to retrieve information about trending public projects and 
    reports the results to a Synapse table.

    The main steps performed in this DAG are:
    1. Query Snowflake to identify public projects, their file sizes, and recent download activities.
    2. Aggregate the results to determine num. of unique users, last download date, and total data size.
    3. Format the results into a list of SnapshotsMetric objects.

    The Snowflake connection and Synapse connection IDs, as well as the time window for recent downloads,
    are provided via DAG parameters.
    """

    @task
    def get_trending_project_snapshot(**context) -> List[SnapshotMetrics]:
        """Execute the query on Snowflake and return the results."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        query = f"""
                WITH PUBLIC_PROJECTS AS (
                    SELECT PROJECT_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST
                    WHERE 1=1
                    AND NODE_TYPE = 'project'
                    AND IS_PUBLIC
                ),
                RECENT_DOWNLOADS AS (
                    SELECT PROJECT_ID, RECORD_DATE, USER_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILEDOWNLOAD
                    WHERE 1=1
                    AND DATE_TRUNC('MONTH', RECORD_DATE) = DATE_TRUNC('MONTH', DATE('{context["params"]["month_to_run"]}'))
                    AND STACK = 'prod'
                    AND PROJECT_ID IN (SELECT PROJECT_ID FROM PUBLIC_PROJECTS)
                ),
                TOP_10_PUBLIC_PROJECTS AS (
                    SELECT PROJECT_ID, COUNT(DISTINCT USER_ID) AS N_UNIQUE_USERS
                    FROM RECENT_DOWNLOADS
                    GROUP BY PROJECT_ID
                    ORDER BY N_UNIQUE_USERS DESC
                    LIMIT 10
                ),
                LATEST_FILE_HANDLES AS (
                    SELECT ID, PROJECT_ID, FILE_HANDLE_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST
                    WHERE 1=1
                    AND NODE_TYPE = 'file'
                    AND PROJECT_ID IN (SELECT PROJECT_ID FROM TOP_10_PUBLIC_PROJECTS)
                ),
                FILE_SIZES AS (
                    SELECT LATEST_FILE_HANDLES.PROJECT_ID, SUM(FILE_LATEST.CONTENT_SIZE) / POWER(1024, 3) AS TOTAL_DATA_SIZE_IN_GIB
                    FROM LATEST_FILE_HANDLES
                    JOIN SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
                    ON LATEST_FILE_HANDLES.FILE_HANDLE_ID = FILE_LATEST.ID
                    GROUP BY LATEST_FILE_HANDLES.PROJECT_ID
                )

                SELECT 
                    TOP_10_PUBLIC_PROJECTS.PROJECT_ID,
                    TOP_10_PUBLIC_PROJECTS.N_UNIQUE_USERS,
                    COALESCE(TO_CHAR(MAX(RECENT_DOWNLOADS.RECORD_DATE), 'YYYY-MM-DD'), 'N/A') AS LAST_DOWNLOAD_DATE,
                    ROUND(FILE_SIZES.TOTAL_DATA_SIZE_IN_GIB, 3) AS TOTAL_DATA_SIZE_IN_GIB
                FROM 
                    TOP_10_PUBLIC_PROJECTS

                LEFT JOIN RECENT_DOWNLOADS
                ON TOP_10_PUBLIC_PROJECTS.PROJECT_ID = RECENT_DOWNLOADS.PROJECT_ID

                JOIN FILE_SIZES
                ON TOP_10_PUBLIC_PROJECTS.PROJECT_ID = FILE_SIZES.PROJECT_ID

                GROUP BY
                    TOP_10_PUBLIC_PROJECTS.PROJECT_ID,
                    TOP_10_PUBLIC_PROJECTS.N_UNIQUE_USERS,
                    FILE_SIZES.TOTAL_DATA_SIZE_IN_GIB

                ORDER BY 
                    TOP_10_PUBLIC_PROJECTS.N_UNIQUE_USERS DESC
            """

        cs.execute(query)
        trending_project_snapshot = cs.fetch_pandas_all()

        metrics = []
        for _, row in trending_project_snapshot.iterrows():
            metrics.append(
                SnapshotMetrics(
                    project_id=row["PROJECT_ID"],
                    n_unique_users=row["N_UNIQUE_USERS"],
                    last_download_date=row["LAST_DOWNLOAD_DATE"],
                    total_data_size_in_gib=row["TOTAL_DATA_SIZE_IN_GIB"],
                )
            )
        return metrics

    @task
    def push_results_to_synapse_table(metrics: List[SnapshotMetrics], **context) -> None:
        """Push the results to a Synapse table."""
        data = []
        export_date = date.today()
        for metric in metrics:
            data.append(
                [
                    metric.project_id,
                    metric.n_unique_users,
                    metric.last_download_date,
                    metric.total_data_size_in_gib,
                    export_date
                ]
            )

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=SYNAPSE_RESULTS_TABLE, values=data)
        )

    project_snapshot = get_trending_project_snapshot()
    push_to_synapse_table = push_results_to_synapse_table(metrics=project_snapshot)

    project_snapshot >> push_to_synapse_table


trending_projects_snapshot()

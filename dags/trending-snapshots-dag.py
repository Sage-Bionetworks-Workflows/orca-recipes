"""
This script is used to execute a query on Snowflake and report the results to a 
Synapse table. This DAG updates the Trending Snapshots Synapse table
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
    "time_window": Param("30", type="string"),
    }

dag_config = {
    # run on the 2nd of every month at midnight
    "schedule_interval": "*/1 * * * *",
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
def trending_snapshots() -> None:
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
    def get_trending_snapshot(**context) -> List[SnapshotMetrics]:
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
                    AND IS_PUBLIC = TRUE
                ),
                LATEST_FILE_HANDLES AS (
                    SELECT DISTINCT ID, PROJECT_ID, FILE_HANDLE_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST
                    WHERE 1=1
                    AND NODE_TYPE = 'file'
                    AND PROJECT_ID IN (SELECT PROJECT_ID FROM PUBLIC_PROJECTS)
                ),
                FILE_SIZES AS (
                    SELECT fh.PROJECT_ID, SUM(f.CONTENT_SIZE) / POWER(1024, 3) AS TOTAL_DATA_SIZE_IN_GIB
                    FROM LATEST_FILE_HANDLES fh
                    JOIN SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST f
                    ON fh.FILE_HANDLE_ID = f.ID
                    GROUP BY fh.PROJECT_ID
                ),
                RECENT_DOWNLOADS AS (
                    SELECT PROJECT_ID, RECORD_DATE, USER_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILEDOWNLOAD
                    WHERE 1=1
                    AND RECORD_DATE > DATEADD(DAY, -{context["params"]["time_window"]}, CURRENT_DATE()) 
                    AND STACK = 'prod'
                    AND PROJECT_ID IN (SELECT PROJECT_ID FROM PUBLIC_PROJECTS)
                )

                SELECT 
                    pp.PROJECT_ID,
                    COUNT(DISTINCT rd.USER_ID) AS N_UNIQUE_USERS,
                    COALESCE(TO_CHAR(MAX(rd.RECORD_DATE), 'YYYY-MM-DD'), 'N/A') AS LAST_DOWNLOAD_DATE,
                    ROUND(fs.TOTAL_DATA_SIZE_IN_GIB, 3) AS TOTAL_DATA_SIZE_IN_GIB
                FROM 
                    PUBLIC_PROJECTS pp

                -- Join RECENT_DOWNLOADS
                LEFT JOIN RECENT_DOWNLOADS rd
                ON pp.PROJECT_ID = rd.PROJECT_ID

                -- Join FILE_SIZES
                LEFT JOIN FILE_SIZES fs
                ON pp.PROJECT_ID = fs.PROJECT_ID

                GROUP BY
                    pp.PROJECT_ID, fs.TOTAL_DATA_SIZE_IN_GIB
                ORDER BY 
                    N_UNIQUE_USERS DESC
                LIMIT 10;
            """

        cs.execute(query)
        trending_snapshot = cs.fetch_pandas_all()

        metrics = []
        for _, row in trending_snapshot.iterrows():
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

    top_downloads = get_trending_snapshot()
    push_to_synapse_table = push_results_to_synapse_table(metrics=top_downloads)

    top_downloads >> push_to_synapse_table


trending_snapshots()

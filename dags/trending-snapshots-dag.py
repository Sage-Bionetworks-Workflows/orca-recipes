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
                -- Subquery: Gets a list of Public Projects on Synapse
                WITH PUBLIC_PROJECTS AS (
                    SELECT PROJECT_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST
                    WHERE 1=1
                    AND NODE_TYPE = 'project'
                    AND IS_PUBLIC = TRUE
                ),
                -- Subquery: Gets the latest file handle IDs for File Entities
                --           belonging to the list of Projects above
                LATEST_FILE_HANDLES AS (
                    SELECT PROJECT_ID, FILE_HANDLE_ID
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST
                    WHERE 1=1
                    AND NODE_TYPE = 'file'
                    AND PROJECT_ID IN (SELECT PROJECT_ID FROM PUBLIC_PROJECTS)
                    AND CHANGE_TIMESTAMP = (
                        SELECT MAX(CHANGE_TIMESTAMP)
                        FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST nl
                        -- This is needed to match the correlated query PROJECT_IDs with the
                        -- outer query PROJECT_IDs
                        WHERE nl.PROJECT_ID = NODE_LATEST.PROJECT_ID
                        AND nl.NODE_TYPE = 'file'
                    )
                ),
                -- Subquery: Gets the total content size of all the File Entities
                FILE_SIZES AS (
                    SELECT fh.PROJECT_ID, SUM(f.CONTENT_SIZE) AS TOTAL_SIZE
                    FROM LATEST_FILE_HANDLES fh
                    JOIN SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST f
                    ON fh.FILE_HANDLE_ID = f.ID
                    GROUP BY fh.PROJECT_ID
                ),
                -- Subquery: Get the file downloads within a given time window
                RECENT_DOWNLOADS AS (
                    SELECT *
                    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILEDOWNLOAD
                    WHERE 1=1
                    AND RECORD_DATE > DATEADD(DAY, -28, '2024-06-29') 
                    AND RECORD_DATE <= '2024-06-29'
                    AND STACK = 'prod'
                    AND PROJECT_ID IS NOT NULL
                )

                SELECT 
                    rd.PROJECT_ID,
                    COUNT(DISTINCT rd.USER_ID) AS UNIQUE_USERS,
                    MAX(rd.RECORD_DATE) AS LAST_DOWNLOAD_DATE,
                    fs.TOTAL_SIZE

                FROM 
                    RECENT_DOWNLOADS rd

                -- Join PUBLIC_PROJECTS
                JOIN PUBLIC_PROJECTS pp
                ON rd.PROJECT_ID = pp.PROJECT_ID

                -- Join FILE_SIZES
                LEFT JOIN FILE_SIZES fs
                ON rd.PROJECT_ID = fs.PROJECT_ID

                -- Sorting the final table for calculation
                GROUP BY
                    rd.PROJECT_ID, fs.TOTAL_SIZE
                ORDER BY 
                    UNIQUE_USERS DESC
                LIMIT 10;
            """

        cs.execute(query)
        top_downloaded_df = cs.fetch_pandas_all()

        metrics = []
        for _, row in top_downloaded_df.iterrows():
            metrics.append(
                SnapshotsMetric(
                    project_id=row["PROJECT_ID"],
                    n_unique_users=row["UNIQUE_USERS"],
                    last_download_date=row["LAST_DOWNLOAD_DATE"],
                    total_data_size_in_gib=row["TOTAL_DATA_SIZE_IN_GIB"],
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

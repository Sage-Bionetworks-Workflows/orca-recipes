"""This script is used to execute a query on Snowflake and report the results to a 
Synapse table. This retrieves data describing the total amount of data hosted in Synapse at the time of execution,
the number of active users in a month, and the number of downloads in a month."""

from dataclasses import dataclass
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook

dag_params = {
    "snowflake_developer_service_conn": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    # date string with the format: YYYY-MM-DD including the month that we want to run the queries for
    "month_to_run": Param((date.today() - relativedelta(months=1)).strftime("%Y-%m-%d"), type="string"),
}

dag_config = {
    # run on the 2nd of every month at midnight
    "schedule_interval": "0 0 2 * *",
    "start_date": datetime(2024, 7, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}

SYNAPSE_RESULTS_TABLE = "syn61588123"


@dataclass
class DownloadMetric:
    """Dataclass to hold the monthly metrics from Synapse.

    Attributes:
        total_data_size_in_pib: The size of the data hosted on Synapse in PiB
        active_users_last_month: The number of active users in the month
        total_downloads_last_month: The total number of downloads by users in the month
    """

    total_data_size_in_pib: float
    active_users_last_month: int
    total_downloads_last_month: int


@dag(**dag_config)
def synapse_by_the_numbers_past_month() -> None:
    """Execute a query to gather the total amount of data hosted in Synapse,
    the number of active users in a month, and the number of downloads
    in a month from Snowflake and report the results to a Synapse table."""

    @task
    def get_synapse_monthly_metrics(**context) -> List[DownloadMetric]:
        """Execute the query on Snowflake and return the results."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        query = f"""
            WITH FILE_SIZES AS (
                SELECT DISTINCT
                    file_latest.id,
                    content_size
                FROM
                    synapse_data_warehouse.synapse.node_latest
                JOIN
                    synapse_data_warehouse.synapse.file_latest
                    ON node_latest.file_handle_id = file_latest.id
                UNION
                SELECT DISTINCT
                    file_latest.id,
                    content_size
                FROM
                    synapse_data_warehouse.synapse.filehandleassociation_latest
                JOIN
                    synapse_data_warehouse.synapse.file_latest
                    ON filehandleassociation_latest.filehandleid = file_latest.id
                WHERE
                    filehandleassociation_latest.associatetype = 'TableEntity'
            ),
            TOTAL_SIZE AS (
                SELECT
                    SUM(content_size) / POWER(2, 50) AS SIZE_IN_PETABYTES
                FROM FILE_SIZES
            ),
            MONTHLY_ACTIVE_USERS AS (
                SELECT 
                    COUNT(DISTINCT user_id) AS DISTINCT_USER_COUNT
                FROM 
                    synapse_data_warehouse.synapse.processedaccess
                WHERE
                    DATE_TRUNC('MONTH', RECORD_DATE) = DATE_TRUNC('MONTH', DATE('{context["params"]["month_to_run"]}'))
            ),
            MONTHLY_DOWNLOADS AS (
                SELECT
                    COUNT(*) AS DOWNLOADS_LAST_MONTH
                FROM (
                    SELECT DISTINCT 
                        user_id,
                        file_handle_id,
                        record_date
                    FROM
                        synapse_data_warehouse.synapse.filedownload
                    WHERE
                        DATE_TRUNC('MONTH', RECORD_DATE) = DATE_TRUNC('MONTH', DATE('{context["params"]["month_to_run"]}'))
                )
            )
            SELECT 
                TOTAL_SIZE.SIZE_IN_PETABYTES,
                MONTHLY_ACTIVE_USERS.DISTINCT_USER_COUNT,
                MONTHLY_DOWNLOADS.DOWNLOADS_LAST_MONTH
            FROM 
                TOTAL_SIZE, MONTHLY_ACTIVE_USERS, MONTHLY_DOWNLOADS;
            """
        print(query)
        cs.execute(query)
        top_downloaded_df = cs.fetch_pandas_all()

        metrics = []
        for _, row in top_downloaded_df.iterrows():
            metrics.append(
                DownloadMetric(
                    total_data_size_in_pib=row["SIZE_IN_PETABYTES"],
                    active_users_last_month=int(row["DISTINCT_USER_COUNT"]),
                    total_downloads_last_month=int(row["DOWNLOADS_LAST_MONTH"]),
                )
            )
        return metrics

    @task
    def push_results_to_synapse_table(metrics: List[DownloadMetric], **context) -> None:
        """Push the results to a Synapse table."""
        data = []
        today = date.today()
        for metric in metrics:
            data.append(
                [
                    today,
                    metric.total_data_size_in_pib,
                    metric.active_users_last_month,
                    metric.total_downloads_last_month,
                ]
            )

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=SYNAPSE_RESULTS_TABLE, values=data)
        )

    top_downloads = get_synapse_monthly_metrics()
    push_to_synapse_table = push_results_to_synapse_table(metrics=top_downloads)

    top_downloads >> push_to_synapse_table


synapse_by_the_numbers_past_month()

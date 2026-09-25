"""Test DAG for synapse-by-the-numbers on MWAA. No schedule — trigger manually."""

from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from src.synapse_hook import SynapseHook

dag_params = {
    "snowflake_developer_service_conn": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "month_to_run": Param((date.today() - relativedelta(months=1)).strftime("%Y-%m-%d"), type="string"),
    "synapse_results_table": Param("syn74496297", type="string"),
}

dag_config = {
    "schedule": None,
    "start_date": datetime(2024, 7, 1),
    "catchup": False,
    "default_args": {"retries": 0},
    "tags": ["snowflake", "test"],
    "params": dag_params,
}


@dag(**dag_config)
def test_synapse_by_the_numbers_past_month() -> None:
    """Test version of synapse_by_the_numbers — manual trigger only, writes to test table."""

    @task
    def get_synapse_monthly_metrics(**context) -> List[dict]:
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
                SELECT SUM(content_size) / POWER(2, 50) AS SIZE_IN_PETABYTES FROM FILE_SIZES
            ),
            MONTHLY_ACTIVE_USERS AS (
                SELECT COUNT(DISTINCT user_id) AS DISTINCT_USER_COUNT
                FROM synapse_data_warehouse.synapse_event.access_event
                WHERE DATE_TRUNC('MONTH', RECORD_DATE) = DATE_TRUNC('MONTH', DATE('{context["params"]["month_to_run"]}'))
            ),
            MONTHLY_DOWNLOADS AS (
                SELECT COUNT(*) AS DOWNLOADS_LAST_MONTH
                FROM (
                    SELECT user_id, file_handle_id, record_date
                    FROM synapse_data_warehouse.synapse_event.objectdownload_event
                    WHERE DATE_TRUNC('MONTH', RECORD_DATE) = DATE_TRUNC('MONTH', DATE('{context["params"]["month_to_run"]}'))
                )
            )
            SELECT
                TOTAL_SIZE.SIZE_IN_PETABYTES,
                MONTHLY_ACTIVE_USERS.DISTINCT_USER_COUNT,
                MONTHLY_DOWNLOADS.DOWNLOADS_LAST_MONTH
            FROM TOTAL_SIZE, MONTHLY_ACTIVE_USERS, MONTHLY_DOWNLOADS;
            """
        cs.execute(query)
        df = cs.fetch_pandas_all()
        return [
            {
                "total_data_size_in_pib": row["SIZE_IN_PETABYTES"],
                "active_users_last_month": int(row["DISTINCT_USER_COUNT"]),
                "total_downloads_last_month": int(row["DOWNLOADS_LAST_MONTH"]),
            }
            for _, row in df.iterrows()
        ]

    @task
    def push_results_to_synapse_table(metrics: list, **context) -> None:
        today = date.today()
        data = [
            [today, m["total_data_size_in_pib"], m["active_users_last_month"], m["total_downloads_last_month"]]
            for m in metrics
        ]
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=context["params"]["synapse_results_table"], values=data)
        )

    top_downloads = get_synapse_monthly_metrics()
    push_results_to_synapse_table(metrics=top_downloads)


dag = test_synapse_by_the_numbers_past_month()

if __name__ == "__main__":
    dag.test(run_conf={"synapse_results_table": "syn74496297"})

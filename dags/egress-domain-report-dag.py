"""Resolve gmail-registered Synapse users' company strings to org domains.

Pipeline steps:
1. Extract every gmail user that has a non-null `company` from Snowflake.
2. Submit Claude batch to resolve each user's company -> organizational domain.
3. Wait for the batch via sensor (reschedule mode).
4. Write (user_id, email, company, resolved_domain) back to a Snowflake table
   (default SAGE.DPE.GMAIL_USER_DOMAIN_RESOLUTION).

The download-aggregation report itself is a SQL query in Snowflake that joins
download events against the resulting lookup table.
"""

import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from src.anthropic_batch_sensor import AnthropicBatchSensor
from src.anthropic_hook import AnthropicHook
from src.egress.egress_domain_report import (
    collect_domain_resolution_results,
    extract_gmail_users_with_company,
    load_to_snowflake,
    submit_domain_resolution_batch,
)

logger = logging.getLogger(__name__)

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "anthropic_conn_id": Param("ANTHROPIC_CONN", type="string"),
    "report_database": Param("SAGE", type="string"),
    "report_schema": Param("DPE", type="string"),
    "report_table": Param("GMAIL_USER_DOMAIN_RESOLUTION", type="string"),
}

dag_config = {
    "schedule": "0 0 1 * *",
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake", "anthropic", "egress"],
    "params": dag_params,
}


def _tmp_path(prefix: str, ts_nodash: str) -> str:
    return f"/tmp/egress_{prefix}_{ts_nodash}.pkl"


@dag(**dag_config)
def egress_domain_report() -> None:
    """Build a gmail-user company -> domain lookup table in Snowflake."""

    @task
    def extract_users(**context) -> str:
        """Query Snowflake for gmail users with a company string."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        conn = snow_hook.get_conn()
        try:
            df = extract_gmail_users_with_company(conn=conn)
        finally:
            conn.close()
        logger.info("Extracted %d gmail users with a company string.", len(df))

        path = _tmp_path("users", context["ts_nodash"])
        df.to_pickle(path)
        return path

    @task
    def submit_batch(users_path: str, **context) -> str:
        """Submit the Claude batch to resolve company -> domain. Returns batch_id."""
        df = pd.read_pickle(users_path)
        anthropic_hook = AnthropicHook(context["params"]["anthropic_conn_id"])
        batch_id = submit_domain_resolution_batch(df, anthropic_hook.client)
        logger.info("Submitted batch: %s", batch_id or "none needed")
        return batch_id

    @task
    def collect_and_load(users_path: str, batch_id: str, **context) -> None:
        """Apply batch results and write the lookup table back to Snowflake."""
        df = pd.read_pickle(users_path)

        anthropic_hook = AnthropicHook(context["params"]["anthropic_conn_id"])
        resolved_df = collect_domain_resolution_results(df, batch_id, anthropic_hook.client)
        n_resolved = resolved_df["resolved_domain"].notna().sum()
        logger.info(
            "Resolved domains for %d / %d users.", n_resolved, len(resolved_df)
        )

        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        conn = snow_hook.get_conn()
        try:
            n_loaded = load_to_snowflake(
                df=resolved_df,
                conn=conn,
                database=context["params"]["report_database"],
                schema=context["params"]["report_schema"],
                table=context["params"]["report_table"],
            )
        finally:
            conn.close()
        logger.info(
            "Loaded %d rows into %s.%s.%s.",
            n_loaded,
            context["params"]["report_database"],
            context["params"]["report_schema"],
            context["params"]["report_table"],
        )

    users_path = extract_users()
    batch_id = submit_batch(users_path)

    wait_for_batch = AnthropicBatchSensor(
        task_id="wait_for_batch",
        batch_id=batch_id,
        anthropic_conn_id="{{ params.anthropic_conn_id }}",
        mode="reschedule",
        poke_interval=60,
    )

    load_task = collect_and_load(users_path, batch_id)
    wait_for_batch.set_downstream(load_task)


dag = egress_domain_report()

if __name__ == "__main__":
    dag.test()

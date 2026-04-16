"""
POC DAG: Load S3 parquet snapshots into Snowflake via Airflow
Spike goal: validate orchestration approach vs. Snowflake-native tools (Tasks)

Record types are defined in record_types.yaml, colocated with this DAG file.
New record types found in the YAML but missing a Snowflake table are created
automatically, and a Slack notification is sent.

Dependencies:
    pip install apache-airflow-providers-snowflake apache-airflow-providers-amazon
    pip install apache-airflow-providers-slack
"""
import os
import requests
import yaml

from datetime import datetime, timedelta

from slack_sdk import WebClient

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENVIRONMENT = os.getenv("RDS_SNAPSHOTS_STACK", "dev")

if ENVIRONMENT.lower() == "prod":
    S3_BUCKET = "synapse-snowflake-rds-snapshots-prod"
    SNOWFLAKE_DATABASE = "SYNAPSE_DATA_WAREHOUSE"
    DAG_SUFFIX = "_prod"
elif ENVIRONMENT.lower() == "dev":
    S3_BUCKET = "synapse-snowflake-rds-snapshots-dev"
    SNOWFLAKE_DATABASE = "SYNAPSE_DATA_WAREHOUSE_DEV_ADD_CLAUDE_MD"
    DAG_SUFFIX = "_dev"
else:
    raise ValueError(f"Unknown environment: {ENVIRONMENT}")

SNOWFLAKE_SCHEMA = "RDS_LANDING"
SNOWFLAKE_S3_STAGE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RDS_SNAPSHOTS_STAGE"
SNOWFLAKE_CONN_ID = "SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN"
AWS_CONN_ID = "AWS_TOWER_PROD_S3_CONN"
SLACK_CHANNEL = "#snowflake_ingest_updates"
SLACK_TOKEN_VAR = "SLACK_DPE_TEAM_BOT_TOKEN"

# ---------------------------------------------------------------------------
# Load record types from YAML
#
# record_types.yaml is fetched from GitHub at DAG parse time.
# Each entry is a record type name matching the S3 subdirectory convention,
# e.g. "ACCESS_APPROVAL" maps to s3://.../rds_snapshots/.../ACCESS_APPROVAL/
#
# Example record_types.yaml:
#   record_types:
#     - ACCESS_APPROVAL
#     - TEAM
#     - RESEARCH_PROJECT
#     - FILE_DOWNLOAD
#     - NODE_SNAPSHOT
#     ... (157 total)
# ---------------------------------------------------------------------------

RECORD_TYPES_URL = "https://raw.githubusercontent.com/Sage-Bionetworks-Workflows/orca-recipes/poc-snapshots-to-snowflake/dags/record_types.yaml"


def load_record_types(url=RECORD_TYPES_URL):
    """Load record types from a raw GitHub URL."""
    response = requests.get(url, timeout=10)
    if not response.ok:
        raise RuntimeError(
            f"Failed to fetch record types... "
            f"Status code: {response.status_code}, reason: {response.reason}"
        )
    return yaml.safe_load(response.text)


_yaml = load_record_types()

RECORD_TYPES: list[str] = _yaml["record_types"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fully_qualified(record_type: str) -> str:
    return f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{record_type}"


def _notify_slack_failure(context):
    client = WebClient(token=Variable.get(SLACK_TOKEN_VAR))
    client.chat_postMessage(
        channel=SLACK_CHANNEL,
        text=(
            f":red_circle: *Ingest failure* in `{context['dag'].dag_id}`\n"
            f"Task: `{context['task_instance'].task_id}`\n"
            f"Run: `{context['run_id']}`\n"
            f"Log: {context['task_instance'].log_url}"
        ),
    )


# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "DPE",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "on_failure_callback": _notify_slack_failure,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id=f"s3_to_snowflake_poc{DAG_SUFFIX}",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["rds", "snapshots", "snowflake", ENVIRONMENT],
)
def s3_to_snowflake_poc() -> None:

    @task
    def notify_started() -> None:
        client = WebClient(token=Variable.get(SLACK_TOKEN_VAR))
        client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f":arrow_forward: *s3_to_snowflake_poc{DAG_SUFFIX}* started — refreshing stage and loading parquet snapshots into `{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}`.",
        )

    @task
    def notify_finished() -> None:
        client = WebClient(token=Variable.get(SLACK_TOKEN_VAR))
        client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f":white_check_mark: *s3_to_snowflake_poc{DAG_SUFFIX}* finished — all COPY INTO tasks completed successfully.",
        )

    @task
    def refresh_stage() -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        ctx = hook.get_conn()
        cs = ctx.cursor()
        try:
            cs.execute(f"ALTER STAGE IF EXISTS {SNOWFLAKE_S3_STAGE} REFRESH;")
        finally:
            cs.close()

    @task
    def create_tables() -> None:
        # Creates all rds_landing tables if they don't already exist.
        # Runs once before the COPY INTO fan-out so every target table is
        # guaranteed to exist when the parallel copy tasks start.
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        ctx = hook.get_conn()
        cs = ctx.cursor()
        try:
            for record_type in RECORD_TYPES:
                cs.execute(
                    f"CREATE TABLE IF NOT EXISTS {_fully_qualified(record_type)} (raw VARIANT);"
                )
        finally:
            cs.close()

    @task
    def copy_record_type(record_type: str) -> None:
        # COPY INTO is idempotent: Snowflake's load history (64-day window,
        # keyed on path + ETag) skips already-loaded files automatically.
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        ctx = hook.get_conn()
        cs = ctx.cursor()
        try:
            cs.execute(f"""
                COPY INTO {_fully_qualified(record_type)} (raw)
                FROM (
                    SELECT $1
                    FROM @{SNOWFLAKE_S3_STAGE}
                )
                FILE_FORMAT = (TYPE = PARQUET)
                PATTERN     = '.*\\.{record_type}/.*\\.parquet'
                ON_ERROR    = 'CONTINUE';
            """)
        finally:
            cs.close()

    # -------------------------------------------------------------------------
    # Pipeline order:
    #
    #   refresh_stage
    #     >> create_tables
    #     >> [copy_access_approval, copy_team, ...]   (parallel)
    #
    #   COPY INTO is the sole dedup mechanism — Snowflake's load history
    #   (path + ETag, 64-day window) skips unchanged files and re-loads
    #   corrected ones automatically. No file sensing needed.
    # -------------------------------------------------------------------------
    started = notify_started()
    stage = refresh_stage()
    tables = create_tables()
    copy_tasks = [
        copy_record_type.override(task_id=f"copy_{rt.lower()}")(rt)
        for rt in RECORD_TYPES
    ]
    finished = notify_finished()

    started >> stage >> tables >> copy_tasks >> finished


s3_to_snowflake_poc()
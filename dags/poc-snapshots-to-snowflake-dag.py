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
import pathlib
import yaml

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

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
    SNOWFLAKE_DATABASE = "SYNAPSE_DATA_WAREHOUSE_DEV"
    DAG_SUFFIX = "_dev"
else:
    raise ValueError(f"Unknown environment: {ENVIRONMENT}")

SNOWFLAKE_SCHEMA = "RDS_LANDING"
SNOWFLAKE_S3_STAGE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RDS_SNAPSHOTS_STAGE"
SNOWFLAKE_CONN_ID = "SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN"
AWS_CONN_ID = "AWS_TOWER_PROD_S3_CONN"
SLACK_CONN_ID = "SLACK_DATA_ENG_CONN"
SLACK_CHANNEL = "#data-eng-alerts"
S3_PREFIX = "rds_snapshots/"

# ---------------------------------------------------------------------------
# Load record types from YAML
#
# record_types.yaml lives in the same directory as this DAG file.
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

_yaml_path = pathlib.Path(__file__).parent / "record_types.yaml"
with _yaml_path.open() as f:
    _yaml = yaml.safe_load(f)

RECORD_TYPES: list[str] = _yaml["record_types"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fully_qualified(record_type: str) -> str:
    return f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{record_type}"


def _check_missing_tables(**context) -> list[str]:
    """
    Query Snowflake's INFORMATION_SCHEMA to find which record types in the
    YAML don't yet have a corresponding table in rds_landing.
    Pushes the list of missing table names to XCom for downstream tasks.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    existing = {
        row[0]
        for row in hook.get_records(
            f"""
            SELECT TABLE_NAME
            FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
            """
        )
    }
    missing = [r for r in RECORD_TYPES if r.upper() not in existing]
    context["ti"].xcom_push(key="missing_tables", value=missing)
    return missing


def _branch_on_missing(**context) -> str | list[str]:
    """
    If there are missing tables, route to the create_tables task group.
    Otherwise skip straight to refresh_stage.
    """
    missing = context["ti"].xcom_pull(
        task_ids="check_missing_tables", key="missing_tables"
    )
    if missing:
        return "handle_new_tables.create_tables"
    return "refresh_stage"


# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id=f"s3_to_snowflake_poc{DAG_SUFFIX}",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["rds", "snapshots", "snowflake", ENVIRONMENT],
) as dag:

    # -------------------------------------------------------------------------
    # 1. Check which YAML-defined record types don't have a table yet.
    # -------------------------------------------------------------------------
    check_missing_tables = PythonOperator(
        task_id="check_missing_tables",
        python_callable=_check_missing_tables,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_missing",
        python_callable=_branch_on_missing,
    )

    # -------------------------------------------------------------------------
    # 2. Handle new record types: create table + notify Slack.
    #    Grouped so the branch target is a single task_id.
    # -------------------------------------------------------------------------
    with TaskGroup("handle_new_tables") as handle_new_tables:

        create_tables = SnowflakeOperator(
            task_id="create_tables",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            # Renders one CREATE TABLE IF NOT EXISTS per missing record type.
            # Adjust column definitions to match your parquet schema.
            sql=[
                f"CREATE TABLE IF NOT EXISTS {_fully_qualified(r)} (raw VARIANT);"
                for r in RECORD_TYPES
            ],
        )

        notify_slack = SlackAPIPostOperator(
            task_id="notify_slack",
            slack_conn_id=SLACK_CONN_ID,
            channel=SLACK_CHANNEL,
            # The message is rendered at runtime from XCom so it lists the
            # actual table names that were created in this run.
            text=(
                ":new: *New rds_landing tables created* (`{{ var.value.get('RDS_SNAPSHOTS_STACK', 'dev') }}` env)\n"
                "{{ task_instance.xcom_pull(task_ids='check_missing_tables', key='missing_tables') | join(', ') }}"
            ),
        )

        create_tables >> notify_slack

    # -------------------------------------------------------------------------
    # 3. Refresh the external stage — updates Snowflake's internal file
    #    pointer cache from S3. No parquet bytes are moved here.
    #    Equivalent to REFRESH_SYNAPSE_WAREHOUSE_S3_STAGE_TASK.
    # -------------------------------------------------------------------------
    refresh_stage = SnowflakeOperator(
        task_id="refresh_stage",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"ALTER STAGE IF EXISTS {SNOWFLAKE_S3_STAGE} REFRESH;",
        trigger_rule="none_failed",  # runs whether or not new tables were created
    )

    # -------------------------------------------------------------------------
    # 4. COPY INTO for each record type — parallel fan-out, equivalent to
    #    the child task graph in the Snowflake-native pattern.
    #
    #    COPY INTO is idempotent: load history on the target table (64-day
    #    window, keyed on path + ETag) skips already-loaded files.
    #    "Query returned no results" = nothing new for that scope, not an error.
    # -------------------------------------------------------------------------
    copy_tasks = []
    for record_type in RECORD_TYPES:
        copy_tasks.append(
            SnowflakeOperator(
                task_id=f"copy_{record_type.lower()}",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=f"""
                    COPY INTO {_fully_qualified(record_type)} (raw)
                    FROM (
                        SELECT $1
                        FROM @{SNOWFLAKE_S3_STAGE}
                    )
                    FILE_FORMAT = (TYPE = PARQUET)
                    PATTERN     = '.*/{record_type}/.*\\\\.parquet'
                    ON_ERROR    = 'CONTINUE';
                """,
            )
        )

    # -------------------------------------------------------------------------
    # 5. dbt test — runs after all scopes load.
    #    Replace with BashOperator or dbt Cloud operator as appropriate.
    # -------------------------------------------------------------------------
    dbt_test = SnowflakeOperator(
        task_id="dbt_test_placeholder",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT 1;",
        # e.g. BashOperator: bash_command="dbt test --select tag:rds_landing"
    )

    # -------------------------------------------------------------------------
    # Pipeline order:
    #
    #   check_missing_tables
    #     >> branch_on_missing
    #     >> [handle_new_tables (create + notify) | skip]
    #     >> refresh_stage          (trigger_rule=none_failed bridges the branch)
    #     >> [copy_access_approval, copy_team, ...]   (parallel)
    #     >> dbt_test
    #
    #   COPY INTO is the sole dedup mechanism — Snowflake's load history
    #   (path + ETag, 64-day window) skips unchanged files and re-loads
    #   corrected ones automatically. No file sensing needed.
    # -------------------------------------------------------------------------
    check_missing_tables >> branch
    branch >> [handle_new_tables, refresh_stage]
    handle_new_tables >> refresh_stage
    refresh_stage >> copy_tasks >> dbt_test
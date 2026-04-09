"""
POC DAG: Load S3 data into Snowflake via Airflow
Spike goal: validate orchestration approach vs. Snowflake-native tools (Tasks)

Dependencies:
    pip install apache-airflow-providers-snowflake apache-airflow-providers-amazon
"""
import os

from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# ---------------------------------------------------------------------------
# Config — Environment-based configuration for dev/prod
# ---------------------------------------------------------------------------

# Environment selection (can be overridden via Airflow Variables or environment variable)
ENVIRONMENT = os.getenv("RDS_SNAPSHOTS_STACK", "dev")  # default to dev

# Configure global variables based on which Synapse platform stack the RDS snapshots are taken from
if ENVIRONMENT.lower() == "prod":
    S3_BUCKET = "synapse-snowflake-rds-snapshots-prod"
    SNOWFLAKE_DATABASE = "SYNAPSE_DATA_WAREHOUSE"
    TARGET_TABLE = "prod_db.prod_schema.rds_snapshots"
    DAG_SUFFIX = "_prod"
elif ENVIRONMENT.lower() == "dev":
    S3_BUCKET = "synapse-snowflake-rds-snapshots-dev"
    SNOWFLAKE_DATABASE = "SYNAPSE_DATA_WAREHOUSE_DEV"
    TARGET_TABLE = "prod_db.prod_schema.rds_snapshots"
    DAG_SUFFIX = "_dev"
else:
    raise ValueError(f"Unknown environment: {ENVIRONMENT}")

SNOWFLAKE_SCHEMA = "RDS_LANDING"
SNOWFLAKE_S3_STAGE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RDS_SNAPSHOTS_STAGE"
SNOWFLAKE_CONN_ID = "SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN"
AWS_CONN_ID = "AWS_TOWER_PROD_S3_CONN"
S3_PREFIX = "rds_snapshots/"

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

    # 1. Refresh the external stage:
    # ALTER STAGE {SNOWFLAKE_S3_STAGE} REFRESH;

    # 2. If there are new files in S3 use COPY INTO to load them into Snowflake staging table
    # How will we do this for each record type on Airflow?

    # 3. DQ check with DBT
    
    # ---------------------------------------------------------------------------
    # Pipeline order
    # ---------------------------------------------------------------------------
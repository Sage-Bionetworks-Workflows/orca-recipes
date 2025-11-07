import os

import snowflake.connector
from snowflake import logger

def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and return a Snowflake connection using environment variables.

    Expected environment variables:
        SNOWFLAKE_USER
        SNOWFLAKE_PASSWORD
        SNOWFLAKE_ACCOUNT
        SNOWFLAKE_DATABASE
        SNOWFLAKE_SCHEMA
        SNOWFLAKE_WAREHOUSE
        SNOWFLAKE_ROLE
    """
    required_vars = [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required Snowflake env vars: {missing}")

    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    logger.info("Connected to Snowflake successfully.")
    return conn

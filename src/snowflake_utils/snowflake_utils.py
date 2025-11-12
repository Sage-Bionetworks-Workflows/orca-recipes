import inspect
import logging
import os

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def create_logger(name: str = "snowflake") -> logging.Logger:
    """Create or retrieve a module-level logger.

    Args:
        name (str): Logger name (default: 'snowflake')

    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)

    # Only configure once (avoids duplicate handlers on import)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger


logger = create_logger("snowflake_utils")


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and return a Snowflake connection using environment variables.

    Expected environment variables:
        SNOWFLAKE_USER
        SNOWFLAKE_ACCOUNT
        SNOWFLAKE_WAREHOUSE
        SNOWFLAKE_ROLE
    """
    required_vars = [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PRIVATE_KEY_FILE",
        "SNOWFLAKE_PRIVATE_KEY_FILE_PWD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required Snowflake env vars: {missing}")

    logger.info("Connecting to Snowflake...")
    if os.getenv("AIRFLOW_CTX_DAG_ID"):
        # Running inside Airflow
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        conn = SnowflakeHook(snowflake_conn_id="snowflake_admin").get_conn()
    else:
        # Running locally
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
            private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
    logger.info("Connected to Snowflake successfully.")
    return conn

    
def table_exists(conn, table_name: str, schema: str | None = None, database: str | None = None) -> bool:
    """Check if a Snowflake table exists."""
    schema = schema or conn.schema
    database = database or conn.database
    if not database or not schema:
        raise ValueError("Must supply both database and schema")

    sql = f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {database}.{schema}"
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchone() is not None
    finally:
        cur.close()

def write_to_snowflake(
    conn,
    table_df: pd.DataFrame,
    table_name: str,
    overwrite: bool = False,
    write_pandas_kwargs: dict | None = None,
) -> None:
    """Write a DataFrame to Snowflake, creating the table if needed.

    Args:
        conn: Snowflake connection
        table_df: DataFrame to upload
        table_name: Snowflake table name
        overwrite: Whether to overwrite the table
        write_pandas_kwargs: Optional dictionary of valid `write_pandas()` args
    """
    # Validate write_pandas kwargs
    valid_params = set(inspect.signature(write_pandas).parameters.keys())
    invalid_keys = []
    if write_pandas_kwargs:
        invalid_keys = [k for k in write_pandas_kwargs if k not in valid_params]
    if invalid_keys:
        raise ValueError(
            f"Invalid write_pandas args: {invalid_keys}. "
            f"Valid options are: {sorted(valid_params)}"
        )

    exists = table_exists(conn, table_name)
    logger.info(
        f"{'Overwriting' if overwrite else 'Appending to' if exists else 'Creating'} table '{table_name}'"
    )

    if exists and not overwrite:
        logger.info(f"Skipping write since '{table_name}' exists and overwrite=False")
        return

    # Set safe defaults
    options = {
        "quote_identifiers": False,
        "auto_create_table": not exists,
        "overwrite": overwrite,
    }
    if write_pandas_kwargs:
        options.update(write_pandas_kwargs)

    success, nchunks, nrows, _ = write_pandas(conn, table_df, table_name, **options)
    if success:
        logger.info(f"Successfully wrote {nrows} rows to '{table_name}' ({nchunks} chunks).")
    else:
        logger.error(f"Failed to write DataFrame to '{table_name}'.")

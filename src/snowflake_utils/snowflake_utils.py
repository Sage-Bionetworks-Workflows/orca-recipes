from contextlib import contextmanager
import inspect
import logging
import os
from typing import Iterator, Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def create_logger(name: str = "snowflake_utils") -> logging.Logger:
    """Create or retrieve a module-level logger.

    Args:
        name (str, optional): Name for the logger. Defaults to "snowflake_utils".

    Returns:
        logging.Logger: configured logger object
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger


logger = create_logger()


def create_local_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Create a Snowflake connection using local environment variables.
    Only used when not running inside Airflow.
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

    logger.info("Connecting to Snowflake locally...")

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
        private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    logger.info("Connected to Snowflake locally.")
    return conn


def get_connection(
    conn: Optional[snowflake.connector.SnowflakeConnection] = None,
) -> snowflake.connector.SnowflakeConnection:
    """Creates a snowflake connection object based on whether there's an input
        connection, otherwise, creates a local connection. This is useful when
        needing to use the connection object.

    Args:
        conn (Optional[snowflake.connector.SnowflakeConnection], optional):
            Snowflake connection object. Defaults to None.

    Returns:
        snowflake.connector.SnowflakeConnection: a snowflake connection
    """
    if conn is not None:
        return conn
    return create_local_connection()


@contextmanager
def get_cursor(
    conn: Optional[snowflake.connector.SnowflakeConnection] = None,
) -> Iterator[snowflake.connector.cursor.SnowflakeCursor]:
    """Yields a Snowflake cursor.
        - If conn is passed (Airflow), we do NOT close it because Airflow's SnowHook
        will handle the connection from there on
        - If conn is None (local), we create one and close the connection.

        This is useful if the code only needs to use the snowflake cursor
        functions (not a connection object)

    Args:
        conn (Optional[snowflake.connector.SnowflakeConnection], optional):
            Input snowflake connection. Defaults to None.

    Yields:
        Iterator[snowflake.connector.cursor.SnowflakeCursor]: the snowflake cursor object
    """
    local_conn = None
    try:
        if conn is None:
            local_conn = create_local_connection()
            conn = local_conn

        with conn.cursor() as cs:
            yield cs

    finally:
        if local_conn is not None:
            local_conn.close()


def table_exists(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    schema: str | None = None,
    database: str | None = None,
) -> bool:
    """Check if a snowflake table exists in the given database and
        schema

    Args:
        conn (snowflake.connector.SnowflakeConnection): snowflake connection object
        table_name (str): name of the snowflake table to check for
        schema (str | None, optional): name of the snowflake schema to check in
        database (str | None, optional: name of the snowflake database to check in

    Returns:
        bool: whether the table exists or not
    """
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
    conn: snowflake.connector.SnowflakeConnection,
    table_df: pd.DataFrame,
    table_name: str,
    overwrite: bool = False,
    write_pandas_kwargs: dict | None = None,
) -> None:
    """Writes a DataFrame to Snowflake under the following conditions

        - If table doesn't exist -> create table and writes to it
        - If table exists + overwrite=False -> skips write to snowflake
        - Otherwise writes to table with snowflake's write_pandas()

    Args:
        conn (snowflake.connector.SnowflakeConnection): Snowflake connection
        table_df (pd.DataFrame): table of the data to write to snowflake
        table_name (str): name of the snowflake table to write to
        overwrite (bool, optional): Whether to overwrite the table or not. Defaults to False.
        write_pandas_kwargs (dict | None, optional): Other write_pandas arguments. Defaults to None.

    Raises:
        ValueError: if invalid parameters are passed to write_pandas
    """
    write_pandas_kwargs = write_pandas_kwargs or {}

    # Validate write_pandas args
    valid_params = set(inspect.signature(write_pandas).parameters.keys())
    invalid_keys = [k for k in write_pandas_kwargs if k not in valid_params]
    if invalid_keys:
        raise ValueError(
            f"Invalid write_pandas args: {invalid_keys}. "
            f"Valid options: {sorted(valid_params)}"
        )

    exists = table_exists(conn, table_name)
    logger.info(
        f"{'Overwriting' if overwrite else 'Appending to' if exists else 'Creating'} table '{table_name}'."
    )

    # Skip write if exists and overwrite=False
    if exists and not overwrite:
        logger.info(f"Skipping write — '{table_name}' exists and overwrite=False")
        return

    options = {
        "auto_create_table": not exists,
        "overwrite": overwrite,
        **write_pandas_kwargs,
    }

    success, nchunks, nrows, _ = write_pandas(conn, table_df, table_name, **options)
    if success:
        logger.info(f"Wrote {nrows} rows to '{table_name}' ({nchunks} chunks).")
    else:
        logger.error(f"Failed to write DataFrame to '{table_name}'.")

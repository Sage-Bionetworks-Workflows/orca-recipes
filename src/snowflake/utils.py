import inspect

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from snowflake import logger

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

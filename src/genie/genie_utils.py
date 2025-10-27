import logging

from snowflake.connector.pandas_tools import write_pandas


def create_logger() -> logging.Logger:
    """This creates a logger for the current process

    Returns:
        logging.Logger: logger for use
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)
    return logger


def table_exists(
    conn, table_name: str, schema: str | None = None, database: str | None = None
) -> bool:
    """Checks if the snowflake table exists

    Args:
        conn (_type_): _description_
        table_name (str): _description_
        schema (str | None, optional): _description_. Defaults to None.
        database (str | None, optional): _description_. Defaults to None.

    Raises:
        ValueError: _description_

    Returns:
        bool: _description_
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
    conn, table_df: pd.DataFrame, table_name: str, overwrite: bool
) -> None:
    """Write a DataFrame to snowflake, creating the table if needed.
        This will skip any table updates if table already exists and overwrite is False

    Args:
        conn (_type_): Snowflake connection
        table_df (pd.DataFrame): Dataframe of the table contents to write
        table_name (str): Name of the table
        overwrite (bool): Whether to overwrite table data or not
    """
    table_exists = table_exists(conn=conn, table_name=table_name)
    log.info(
        f"{'Overwriting' if overwrite else 'Appending to' if table_exists else 'Creating'} table '{table_name}'"
    )
    # skips any table updates
    if table_exists and not overwrite:
        pass
    else:
        write_pandas(
            conn=conn,
            df=table_df,
            table_name=table_name,
            quote_identifiers=False,
            overwrite=overwrite,
            auto_create_table=not table_exists,
        )

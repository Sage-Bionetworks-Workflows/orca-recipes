import argparse
import os
import re
from datetime import datetime, timezone
from typing import NamedTuple

import pandas as pd
import synapseclient
import yaml
from snowflake.connector.pandas_tools import write_pandas

from snowflake_utils import get_connection, logger

CLINICAL_SCHEMA = "BPC_CLINICAL_FILES"
CBIOPORTAL_SCHEMA = "BPC_CBIOPORTAL_FILES"

CBIOPORTAL_EXCLUDE_PREFIXES = (
    "data_gene_panel",
    "meta",
    "data_CNA",
    "case_lists",
    "data_seg",
    "tmb",
)


# Synapse filename -> Snowflake table
CBIOPORTAL_FILES = {
    "data_clinical_sample.txt": "CLINICAL_SAMPLE",
}

CLINICAL_FILES = {
    "cancer_panel_test_level_dataset.csv": "CANCER_PANEL_TEST_LEVEL",
}


class ReleaseInfo(NamedTuple):
    """
    Information parsed from a genie release identifier.

    Attributes
    ----------
    cohort : str
        Name of the cohort (e.g: CRC)
    release : str
        Full release string (e.g., "17.1-consortium", "17.0-public").
    release_type : str
        Type of release usually just "public" or "consortium"
    major_version : int
        Major version number (e.g. 17).
    minor_version : int
        Minor version number (e.g. 2).
    """

    cohort: str
    release: str
    release_type: str
    major_version: int
    minor_version: int


def parse_bpc_version(cohort: str, version: str) -> ReleaseInfo:
    """Parses the ReleaseInfo information from the release folder
    Expected format:
      public_02_2
      consortium_03_1

    Args:
        cohort (str): Name of the cohort
        version (str): Version of the

    Raises:
        ValueError: raises error when version format doesn't follow the above expected

    Returns:
        ReleaseInfo: object containing the release info details
    """
    m = re.match(r"^(public|consortium)_(\d+)_(\d+)$", version, flags=re.IGNORECASE)
    if not m:
        raise ValueError(f"Unexpected BPC version format: {version}")

    release_type = m.group(1).upper()
    major_v = int(m.group(2))
    minor_v = int(m.group(3))

    release = f"{major_v}_{minor_v}_{release_type}"
    return ReleaseInfo(
        cohort=cohort,
        release=release,
        release_type=release_type,
        major_version=major_v,
        minor_version=minor_v,
    )


def ensure_schema(
    conn: "snowflake.connector.SnowflakeConnection", database: str, schema: str
) -> None:
    """Creates the Snowflake schema in the given database

    Args:
        conn (snowflake.connector.SnowflakeConnection): Snowflake connection
        database (str): Name of the Snowflake database to create schema in
        schema (str): Name of the schema to create in the Snowflake database
    """
    with conn.cursor() as cs:
        cs.execute(f"USE DATABASE {database};")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema} WITH MANAGED ACCESS;")
        cs.execute(f"USE SCHEMA {schema};")
    logger.info(f"Using schema: {database}.{schema}")


def partition_exists(
    conn: "snowflake.connector.SnowflakeConnection",
    table: str,
    cohort: str,
    release: str,
) -> bool:
    """
    Returns True if the table already has at least one row for (COHORT, RELEASE).
    """
    with conn.cursor() as cs:
        cs.execute(
            f"SELECT 1 FROM {table} WHERE COHORT = %s AND RELEASE = %s LIMIT 1",
            (cohort, release),
        )
        return cs.fetchone() is not None


def delete_existing_partition(
    conn: "snowflake.connector.SnowflakeConnection",
    table: str,
    cohort: str,
    release: str,
) -> None:
    """Deletes the partition of the Snowflake table filtering on
        the release and cohort.
        This is only used when overwriting existing partitions.

    Args:
        conn (snowflake.connector.SnowflakeConnection): Snowflake connection
        table (str): Name of the Snowflake table to delete from
        cohort (str): Name of the cohort to filter on
        release (str): Name of the release to filter on
    """
    with conn.cursor() as cs:
        cs.execute(
            f"DELETE FROM {table} WHERE COHORT = %s AND RELEASE = %s",
            (cohort, release),
        )


def append_df(
    conn: "snowflake.connector.SnowflakeConnection", df: pd.DataFrame, table: str
) -> None:
    """Appends the table to snowflake. We only append when uploading results
    (using overwrite = False) here because if we overwrite Snowflake tables,
    Snowflake will overwrite everything to just include this data from a specific release
    but because we append multiple releases in a table, we wouldn't want that.
    We'd just want to overwrite each release which we handle in delete_existing_partition.

    Args:
        conn (snowflake.connector.SnowflakeConnection): Snowflake connection
        df (pd.DataFrame): the input data to append to a Snowflake table
        table (str): name of the Snowflake table to append to

    Raises:
        RuntimeError: making sure the code fails even for silent errors
    """
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table,
        auto_create_table=True,
        overwrite=False,
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for table {table}")
    logger.info(f"Appended {nrows} rows to {table} ({nchunks} chunks).")


def add_bpc_metadata(
    df: pd.DataFrame,
    release_info: ReleaseInfo,
    source_file: str,
) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        release_info (ReleaseInfo): _description_
        source_file (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df = df.copy()
    df["COHORT"] = release_info.cohort.upper()
    df["RELEASE"] = release_info.release
    df["RELEASE_TYPE"] = release_info.release_type
    df["MAJOR_VERSION"] = release_info.major_version
    df["MINOR_VERSION"] = release_info.minor_version
    df["SOURCE_FILE"] = source_file
    df["INGESTED_AT"] = datetime.now(timezone.utc)
    return df


def upload_clinical_tables_stacked(
    conn: "snowflake.connector.SnowflakeConnection",
    syn: synapseclient.Synapse,
    database: str,
    release_info: ReleaseInfo,
    clinical_synid: str,
    overwrite_partition: bool,
) -> None:
    """_summary_

    Args:
        conn (snowflake.connector.SnowflakeConnection): _description_
        syn (synapseclient.Synapse): _description_
        database (str): _description_
        release_info (ReleaseInfo): _description_
        clinical_synid (str): _description_
        overwrite_partition (bool): _description_
    """
    ensure_schema(conn, database=database, schema=CLINICAL_SCHEMA)

    for clinical_file in syn.getChildren(clinical_synid):
        raw_name = clinical_file["name"]
        if not raw_name.endswith(".csv") or raw_name not in CLINICAL_FILES:
            continue

        table = CLINICAL_FILES[raw_name]
        ent = syn.get(clinical_file["id"])

        df = pd.read_csv(ent.path, sep=",", comment="#", low_memory=False)
        if df.empty:
            logger.warning(
                f"[{release_info.cohort} {release_info.release}] Empty clinical file: {raw_name}"
            )
            continue

        # remove cohort column to avoid dups if it exists
        if "cohort" in df.columns:
            df = df.drop(columns=["cohort"])

        df = add_bpc_metadata(df, release_info=release_info, source_file=raw_name)

        if overwrite_partition:
            delete_existing_partition(
                conn,
                table=f"{CLINICAL_SCHEMA}.{table}",
                cohort=release_info.cohort,
                release=release_info.release,
            )
        else:
            # If partition already exists, do nothing
            if partition_exists(
                conn,
                table=f"{CLINICAL_SCHEMA}.{table}",
                cohort=release_info.cohort,
                release=release_info.release,
            ):
                logger.info(
                    f"[{release_info.cohort} {release_info.release}] "
                    f"Partition already exists in {table}; skipping (overwrite_partition=False)."
                )
                continue
        append_df(conn, df=df, table=table)


def upload_cbioportal_tables_stacked(
    conn: "snowflake.connector.SnowflakeConnection",
    syn: synapseclient.Synapse,
    database: str,
    release_info: ReleaseInfo,
    cbioportal_synid: str,
    overwrite_partition: bool,
) -> None:
    """_summary_

    Args:
        conn (snowflake.connector.SnowflakeConnection): _description_
        syn (synapseclient.Synapse): _description_
        database (str): _description_
        release_info (ReleaseInfo): _description_
        cbioportal_synid (str): _description_
        overwrite_partition (bool): _description_
    """
    ensure_schema(conn, database=database, schema=CBIOPORTAL_SCHEMA)

    cohort = release_info.cohort

    for cbioportal_file in syn.getChildren(cbioportal_synid):
        raw_name = cbioportal_file["name"]

        if (
            raw_name.startswith(CBIOPORTAL_EXCLUDE_PREFIXES)
            or raw_name not in CBIOPORTAL_FILES
        ):
            continue

        table = CBIOPORTAL_FILES[raw_name]

        sep = "\t" if raw_name.endswith(".txt") else ","
        ent = syn.get(cbioportal_file["id"])

        df = pd.read_csv(ent.path, sep=sep, comment="#", low_memory=False)
        if df.empty:
            logger.warning(
                f"[{cohort} {release_info.release}] Empty cbioportal file: {raw_name}"
            )
            continue

        df = add_bpc_metadata(df, release_info=release_info, source_file=raw_name)

        if overwrite_partition:
            delete_existing_partition(
                conn,
                table_fqn=f"{CBIOPORTAL_SCHEMA}.{table}",
                cohort=cohort,
                release=release_info.release,
            )

        append_df(conn, df=df, table=table)


def push_bpc_release_to_snowflake(
    syn: synapseclient.Synapse,
    conn: "snowflake.connector.SnowflakeConnection",
    database: str,
    cohort: str,
    version: str,
    clinical_synid: str,
    cbioportal_synid: str,
    overwrite_partition: bool,
) -> None:
    """_summary_

    Args:
        syn (synapseclient.Synapse): _description_
        conn (snowflake.connector.SnowflakeConnection): _description_
        database (str): _description_
        cohort (str): _description_
        version (str): _description_
        clinical_synid (str): _description_
        cbioportal_synid (str): _description_
        overwrite_partition (bool): _description_
    """
    release_info = parse_bpc_version(cohort=cohort, version=version)
    logger.info(
        f"Processing cohort={release_info.cohort}, release={release_info.release}"
    )

    upload_clinical_tables_stacked(
        conn=conn,
        syn=syn,
        database=database,
        release_info=release_info,
        clinical_synid=clinical_synid,
        overwrite_partition=overwrite_partition,
    )

    upload_cbioportal_tables_stacked(
        conn=conn,
        syn=syn,
        database=database,
        release_info=release_info,
        cbioportal_synid=cbioportal_synid,
        overwrite_partition=overwrite_partition,
    )


def main(database: str, overwrite_partition: bool, conn=None) -> None:
    """_summary_

    Args:
        database (str): _description_
        overwrite_partition (bool): _description_
        conn (_type_, optional): _description_. Defaults to None.
    """
    syn = synapseclient.login()

    script_dir = os.path.dirname(__file__)
    yaml_path = os.path.join(script_dir, "genie_bpc_releases.yaml")

    with open(yaml_path, "r") as f:
        cohorts = yaml.safe_load(f)

    conn_obj = get_connection(conn=conn)
    logger.info("Connected to Snowflake.")

    try:
        for cohort_info in cohorts:
            if cohort_info["version"].startswith("public_preview"):
                continue
            push_bpc_release_to_snowflake(
                syn=syn,
                conn=conn_obj,
                database=database,
                cohort=cohort_info["cohort"],
                version=cohort_info["version"],
                clinical_synid=cohort_info["clinical_synid"],
                cbioportal_synid=cohort_info["cbioportal_synid"],
                overwrite_partition=overwrite_partition,
            )

        logger.info("All cohorts processed successfully.")
    finally:
        if conn is None:
            conn_obj.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the GENIE BPC ELT pipeline (stacked)."
    )
    parser.add_argument(
        "--database",
        type=str,
        default="GENIE_DEV",
        choices=["GENIE", "GENIE_DEV"],
        help="Database to run ELT script in.",
    )
    parser.add_argument(
        "--overwrite-partition",
        action="store_true",
        help="Delete existing rows for (COHORT, RELEASE) before appending.",
    )
    args = parser.parse_args()

    main(
        database=args.database,
        overwrite_partition=args.overwrite_partition,
    )

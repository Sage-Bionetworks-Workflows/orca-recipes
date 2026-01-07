#!/usr/bin/env python
"""MAIN GENIE ingestion pipeline"""

import argparse
import re
from datetime import datetime, timezone
from typing import Dict, NamedTuple


import pandas as pd
import synapseclient
import synapseutils as synu
from snowflake.connector.pandas_tools import write_pandas

from snowflake_utils import get_connection, logger

# releases too old and not categorized into public vs consortium
RELEASES_TO_SKIP = [
  "Release 00", "Release 01", "Release 02", "Release 03",
  "Release 04", "Release 05", "Release 06", "Release 07",
  "Release 08", "Release 09", "Release 10", "Release 11",
  "Release 12", "Release 13", "Release 14", "Release 15",
  "Release 16",
]

PROJECT_SYNID = "syn7492881"

# Only ingest these (Synapse filenames) for this script.
# Easy to extend later: add more filenames and map them to target tables.
FILEFORMATS = {
    "data_clinical_sample.txt": "CLINICAL_SAMPLE",  # Synapse filename -> Snowflake table
}


class ReleaseInfo(NamedTuple):
    release: str
    release_type: str
    major_version: int
    minor_version: int


def parse_release_folder(folder_name: str) -> ReleaseInfo:
    """Synapse release folder name is typically: {major}.{minor}-{release_type}
      e.g. "19.3-consortium"

    Args:
        folder_name (str): _description_

    Raises:
        ValueError: _description_
        ValueError: _description_

    Returns:
        ReleaseInfo(release, release_type, major_version, minor_version)
    """
    parts = folder_name.split("-")
    if len(parts) != 2:
        raise ValueError(f"Unexpected folder name format: {folder_name}")

    version_raw = parts[0].replace(".", "_")
    release_type = parts[1].upper()

    m = re.match(r"^(\d+)[_](\d+)$", version_raw)
    if not m:
        raise ValueError(f"Unexpected version format: {folder_name}")

    major_v = int(m.group(1))
    minor_v = int(m.group(2))
    release = f"{major_v}_{minor_v}_{release_type}"

    return ReleaseInfo(
        release=release,
        release_type=release_type,
        major_version=major_v,
        minor_version=minor_v,
    )


def get_release_file_map(
    syn: synapseclient.Synapse,
    release_folder_synid: str,
) -> Dict[str, synapseclient.Entity]:
    """
    Return mapping: {filename -> Synapse entity} for only the fileformats we ingest.
    """
    release_files = syn.getChildren(
        release_folder_synid, includeTypes=["file", "folder", "link"]
    )
    wanted = set(FILEFORMATS.keys())
    release_file_map : Dict[str, synapseclient.Entity] = {
        release_file["name"]: syn.get(release_file["id"], followLink=True)
        for release_file in release_files
        if release_file["name"] in wanted
    }
    return release_file_map


def ensure_schema(
    conn: "snowflake.connector.SnowflakeConnection", database: str, schema: str
) -> None:
    """_summary_

    Args:
        conn (snowflake.connector.SnowflakeConnection): _description_
        database (str): _description_
        schema (str): _description_
    """
    with conn.cursor() as cs:
        cs.execute(f"USE DATABASE {database};")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema} WITH MANAGED ACCESS;")
        cs.execute(f"USE SCHEMA {schema};")
        logger.info(f"Using schema: {schema}")


def delete_existing_partition(
    conn: "snowflake.connector.SnowflakeConnection", table: str, release: str
) -> None:
    """_summary_

    Args:
        conn (snowflake.connector.SnowflakeConnection): _description_
        table (str): _description_
        release (str): _description_
    """
    with conn.cursor() as cs:
        cs.execute(f"DELETE FROM {table} WHERE RELEASE = %s", (release,))


def append_df(
    conn: "snowflake.connector.SnowflakeConnection", df: pd.DataFrame, table: str
) -> None:
    """_summary_

    Args:
        conn (snowflake.connector.SnowflakeConnection): _description_
        df (pd.DataFrame): _description_
        table (str): _description_

    Raises:
        RuntimeError: _description_
    """
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table,
        auto_create_table=True,
        overwrite=False,  # append
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for table {table}")
    logger.info(f"Appended {nrows} rows to '{table}' ({nchunks} chunks).")


def is_valid_release_path(release_path: str) -> bool:
    """Checks that the current release path
    is valid.

    - Expects form of  {top_level_release_folder}/{release_name}
        as the release path, e.g: "Releases/Release 0"
    - Expects a newer release not present in RELEASES_TO_SKIP

    Args:
        release_path (str): the input release path to check for
            validity

    Returns:
        bool: whether the release path is valid to iterate through
            or not
    """
    path = release_path[0]
    parts = path.split("/")
    if len(parts) != 2:
        return False
    if parts[1] in RELEASES_TO_SKIP:
        return False
    return True


def push_release_to_snowflake(
    syn: synapseclient.Synapse,
    conn: "snowflake.connector.SnowflakeConnection",
    release_folder_synid: str,
    release_folder_name: str,
    database: str,
    overwrite_partition: bool,
) -> None:
    """For one release folder:
      - find the file(s) we care about
      - stack into MAIN.<TARGET_TABLE>

    Args:
        syn (synapseclient.Synapse): _description_
        conn (snowflake.connector.SnowflakeConnection): _description_
        release_folder_synid (str): _description_
        release_folder_name (str): _description_
        database (str): _description_
        overwrite_partition (bool): _description_
    """
    release_info = parse_release_folder(release_folder_name)
    file_map = get_release_file_map(syn, release_folder_synid)
    if not file_map:
        logger.info(
            f"[{release_folder_name}] No configured fileformats found; skipping."
        )
        return

    # Ensure we are in the project-level schema
    ensure_schema(conn, database=database, schema="MAIN")

    for syn_filename, ent in file_map.items():
        target_table = FILEFORMATS[syn_filename]

        df = pd.read_csv(ent.path, sep="\t", comment="#", low_memory=False)
        if df.empty:
            logger.warning(
                f"[{release_folder_name}] Empty file {syn_filename}; skipping."
            )
            continue

        # Add release metadata
        df["RELEASE"] = release_info.release
        df["RELEASE_TYPE"] = release_info.release_type
        df["MAJOR_VERSION"] = release_info.major_version
        df["MINOR_VERSION"] = release_info.minor_version
        df["INGESTED_AT"] = datetime.now(timezone.utc)
        if overwrite_partition:
            delete_existing_partition(
                conn, table=target_table, release=release_info.release
            )

        append_df(conn, df=df, table=target_table)
        logger.info(
            f"[{release_folder_name}] Loaded {syn_filename} into MAIN.{target_table} (RELEASE={release_info.release})."
        )


def main(database: str, overwrite_partition: bool, conn=None) -> None:
    """
    Main function -loops through the
        top level releases project folder, checks for
        valid release paths and runs the function to
        ingest those data into snowflake

    Args:
        database (str): Database table to run ELT commands in
        overwrite_partition (bool): Whether to overwrite table data or not
        conn (snowflake.connector.SnowflakeConnection): Optional Snowflake
            connection injected externally (e.g. Airflow)
    """
    syn = synapseclient.login()
    conn_obj = get_connection(conn=conn)

    try:
        logger.info("Connected to Snowflake.")
        for dirpath, dirnames, _ in synu.walk(syn, PROJECT_SYNID):
            if not is_valid_release_path(release_path=dirpath):
                logger.info(f"Skipping invalid release path: {dirpath}.")
                continue

            for release_folder_name, release_folder_synid in dirnames:
                logger.info(f"Processing release folder: {release_folder_name}")
                push_release_to_snowflake(
                    syn=syn,
                    conn=conn_obj,
                    release_folder_synid=release_folder_synid,
                    release_folder_name=release_folder_name,
                    database=database,
                    overwrite_partition=overwrite_partition,
                )

        logger.info("Ingestion completed successfully.")
    finally:
        # if it's a local connection, close it
        if conn is None:
            conn_obj.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MAIN GENIE ingestion pipeline.")
    parser.add_argument(
        "--database",
        type=str,
        default="GENIE_DEV",
        choices=["GENIE", "GENIE_DEV"],
        help="Database to run ingestion commands in.",
    )
    parser.add_argument(
        "--overwrite-partition",
        action="store_true",
        help="Delete existing rows for a RELEASE before appending (recommended for idempotent reruns).",
    )
    args = parser.parse_args()
    main(database=args.database, overwrite_partition=args.overwrite_partition)

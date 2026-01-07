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

CBIOPORTAL_EXCLUDE_PREFIXES = ("data_gene_panel", "meta", "data_CNA", "case_lists", "data_seg", "tmb")


# Synapse filename -> Snowflake table
CBIOPORTAL_FILES = {
    "data_clinical_sample.txt": "CLINICAL_SAMPLE",
}

CLINICAL_FILES = {
    "cancer_panel_test_level_dataset.csv": "CANCER_PANEL_TEST_LEVEL",
}


class ReleaseInfo(NamedTuple):
    cohort: str
    release: str
    release_type: str
    major_version: int
    minor_version: int


def parse_bpc_version(cohort: str, version: str) -> ReleaseInfo:
    """
    Expected format:
      public_02_2
      consortium_03_1
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


def ensure_schema(conn, database: str, schema: str) -> None:
    with conn.cursor() as cs:
        cs.execute(f"USE DATABASE {database};")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema} WITH MANAGED ACCESS;")
        cs.execute(f"USE SCHEMA {schema};")
    logger.info(f"Using schema: {database}.{schema}")


def delete_existing_partition(conn, table_fqn: str, cohort: str, release: str) -> None:
    with conn.cursor() as cs:
        cs.execute(
            f"DELETE FROM {table_fqn} WHERE COHORT = %s AND RELEASE = %s",
            (cohort, release),
        )


def append_df(conn, df: pd.DataFrame, table: str) -> None:
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


# ----------------------------
# Naming + metadata
# ----------------------------
def get_clinical_table_name(filename: str) -> str:
    return filename.replace(".csv", "")


def get_cbioportal_table_name(filename: str, cohort: str) -> str:
    return (
        filename.replace("data_", "")
        .replace(".txt", "")
        .replace(f"genie_{cohort}_", "")
        .replace("cna_hg19.seg", "seg")
    )


def add_bpc_metadata(
    df: pd.DataFrame,
    release_info: ReleaseInfo,
    source_file: str,
) -> pd.DataFrame:
    df = df.copy()
    df["COHORT"] = release_info.cohort.upper()
    df["RELEASE"] = release_info.release
    df["RELEASE_TYPE"] = release_info.release_type
    df["MAJOR_VERSION"] = release_info.major_version
    df["MINOR_VERSION"] = release_info.minor_version
    df["SOURCE_FILE"] = source_file
    df["INGESTED_AT"] = datetime.now(timezone.utc)
    return df


# ----------------------------
# Ingestion routines
# ----------------------------
def upload_clinical_tables_stacked(
    conn,
    syn: synapseclient.Synapse,
    database: str,
    release_info: ReleaseInfo,
    clinical_synid: str,
    overwrite_partition: bool,
) -> None:
    ensure_schema(conn, database=database, schema=CLINICAL_SCHEMA)

    for clinical_file in syn.getChildren(clinical_synid):
        raw_name = clinical_file["name"]
        if not raw_name.endswith(".csv") or raw_name not in CLINICAL_FILES:
            continue

        #table = get_clinical_table_name(raw_name)
        table = CLINICAL_FILES[raw_name]
        ent = syn.get(clinical_file["id"])

        df = pd.read_csv(ent.path, sep=",", comment="#", low_memory=False)
        if df.empty:
            logger.warning(f"[{release_info.cohort} {release_info.release}] Empty clinical file: {raw_name}")
            continue
        
        # remove cohort column to avoid dups if it exists
        if "cohort" in df.columns:
            df = df.drop(columns = ["cohort"])
            
        df = add_bpc_metadata(df, release_info=release_info, source_file=raw_name)

        if overwrite_partition:
            delete_existing_partition(
                conn,
                table_fqn=f"{CLINICAL_SCHEMA}.{table}",
                cohort=release_info.cohort,
                release=release_info.release,
            )

        append_df(conn, df=df, table=table)


def upload_cbioportal_tables_stacked(
    conn,
    syn: synapseclient.Synapse,
    database: str,
    release_info: ReleaseInfo,
    cbioportal_synid: str,
    overwrite_partition: bool,
) -> None:
    ensure_schema(conn, database=database, schema=CBIOPORTAL_SCHEMA)

    cohort = release_info.cohort

    for cbioportal_file in syn.getChildren(cbioportal_synid):
        raw_name = cbioportal_file["name"]

        if raw_name.startswith(CBIOPORTAL_EXCLUDE_PREFIXES) or raw_name not in CBIOPORTAL_FILES:
            continue
        
        #table = get_cbioportal_table_name(raw_name, cohort)
        table = CBIOPORTAL_FILES[raw_name]

        sep = "\t" if raw_name.endswith(".txt") else ","
        ent = syn.get(cbioportal_file["id"])

        df = pd.read_csv(ent.path, sep=sep, comment="#", low_memory=False)
        if df.empty:
            logger.warning(f"[{cohort} {release_info.release}] Empty cbioportal file: {raw_name}")
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
    syn,
    conn,
    database: str,
    cohort: str,
    version: str,
    clinical_synid: str,
    cbioportal_synid: str,
    overwrite_partition: bool,
) -> None:
    release_info = parse_bpc_version(cohort=cohort, version=version)
    logger.info(f"Processing cohort={release_info.cohort}, release={release_info.release}")

    upload_clinical_tables_stacked(
        conn=conn,
        syn=syn,
        database=database,
        release_info=release_info,
        clinical_synid=clinical_synid,
        overwrite_partition=overwrite_partition,
    )

    #upload_cbioportal_tables_stacked(
    #    conn=conn,
    #    syn=syn,
    #    database=database,
    #    release_info=release_info,
    #    cbioportal_synid=cbioportal_synid,
    #    overwrite_partition=overwrite_partition,
    #)


def main(database: str, overwrite_partition: bool, conn=None) -> None:
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
    parser = argparse.ArgumentParser(description="Run the GENIE BPC ELT pipeline (stacked).")
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

#!/usr/bin/env python
"""
ELT pipeline for sponsored project clinical data.
Reads YAML config for cohorts, fetches Synapse files, and writes to Snowflake.
"""

import argparse
import os

import yaml
import pandas as pd
import synapseclient

from snowflake_utils import get_connection, write_to_snowflake, logger


def process_cohort(
    syn: synapseclient.Synapse,
    conn,
    cohort_config: dict,
    database: str,
    overwrite: bool,
):
    """
    Process a single cohort:
      - Download files from Synapse
      - Use specified patient_id_key and sample_id_key
      - Write to Snowflake table named in YAML
    """
    schema_name = cohort_config["cohort"]
    table_name = cohort_config["table_name"]
    folder_synids = cohort_config["folder_synid"]

    logger.info(f"Creating/using schema: {schema_name}")

    with get_connection(conn) as conn:
        with conn.cursor() as cs:
            cs.execute(f"USE DATABASE {database};")
            cs.execute(
                f"CREATE SCHEMA IF NOT EXISTS {schema_name} WITH MANAGED ACCESS;"
            )
            cs.execute(f"USE SCHEMA {schema_name}")

        # Ensure folder_synids is a list
        if not isinstance(folder_synids, list):
            folder_synids = [folder_synids]
            overwrite = False

        for synid in folder_synids:
            logger.info(f"[{schema_name}] Processing Synapse folder: {synid}")
            # Download the files from Synapse folder
            folder_files = syn.getChildren(synid, includeTypes=["file"])
            for f in folder_files:
                file_entity = syn.get(f["id"], downloadLocation="/tmp", followLink=True)
                df = pd.read_csv(file_entity.path, sep="\t", low_memory=False)

                # Write DataFrame to Snowflake
                write_to_snowflake(
                    conn=conn,
                    table_df=df,
                    table_name=table_name,
                    overwrite=overwrite,
                    quote_identifiers=False,
                )
                logger.info(
                    f"[{schema_name}] Wrote {len(df)} rows to table '{table_name}'"
                )


def main(
    database: str,
    overwrite: bool,
    conn: "snowflake.connector.SnowflakeConnection" = None,
):
    # Connect to Synapse
    syn = synapseclient.login()

    script_dir = os.path.dirname(__file__)
    yaml_path = os.path.join(script_dir, "genie_bpc_releases.yaml")
    with open(yaml_path, "r") as f:
        cohort_configs = yaml.safe_load(f)
        for cohort_config in cohort_configs:
            process_cohort(syn, conn, cohort_config, database, overwrite)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the GENIE SP ELT pipeline.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite table data if present.",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="GENIE_DEV",
        help="Database to run ELT script in.",
    )
    args = parser.parse_args()
    main(database=args.database, overwrite=args.overwrite)

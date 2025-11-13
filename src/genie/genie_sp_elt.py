#!/usr/bin/env python
"""
ELT pipeline for sponsored project clinical data.
Reads YAML config for cohorts, fetches Synapse files, and writes to Snowflake.
"""

import argparse
import yaml
import pandas as pd
import synapseclient
from snowflake_utils import get_connection, write_to_snowflake, logger

def load_cohort_config(yaml_path: str):
    """Load the cohort YAML configuration."""
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)


def process_cohort(syn: synapseclient.Synapse, conn, cohort_config: dict):
    """
    Process a single cohort:
      - Download files from Synapse
      - Use specified patient_id_key and sample_id_key
      - Write to Snowflake table named in YAML
    """
    cohort = cohort_config["cohort"]
    table_name = cohort_config["table_name"]
    folder_synids = cohort_config["folder_synid"]
    
    # Ensure folder_synids is a list
    if not isinstance(folder_synids, list):
        folder_synids = [folder_synids]
    
    for synid in folder_synids:
        logger.info(f"[{cohort}] Processing Synapse folder: {synid}")
        # Download the files from Synapse folder
        folder_files = syn.getChildren(synid, includeTypes=["file"])
        for f in folder_files:
            file_entity = syn.get(f["id"], downloadLocation="/tmp", followLink=True)
            try:
                df = pd.read_csv(file_entity.path, sep="\t", low_memory=False)
            except pd.errors.EmptyDataError:
                logger.warning(f"{file_entity.path} is empty — skipping.")
                continue
            except Exception as e:
                logger.error(f"Failed to read {file_entity.path}: {e}")
                continue

            # Optionally rename columns based on patient_id_key/sample_id_key
            if cohort_config.get("patient_id_key") and cohort_config.get("sample_id_key"):
                df = df.rename(
                    columns={
                        cohort_config["patient_id_key"]: "PATIENT_ID",
                        cohort_config["sample_id_key"]: "SAMPLE_ID",
                    }
                )

            # Write DataFrame to Snowflake
            write_to_snowflake(conn=conn, table_df=df, table_name=table_name, overwrite=True)
            logger.info(f"[{cohort}] Wrote {len(df)} rows to table '{table_name}'")


def main(yaml_path: str):
    # Connect to Synapse
    syn = synapseclient.login()

    # Connect to Snowflake
    with get_connection() as conn:
        cohort_configs = load_cohort_config(yaml_path)
        for cohort_config in cohort_configs:
            process_cohort(syn, conn, cohort_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run sponsored project clinical data ELT.")
    parser.add_argument(
        "--yaml",
        required=True,
        help="Path to the cohort YAML configuration file",
    )
    args = parser.parse_args()
    main(args.yaml)

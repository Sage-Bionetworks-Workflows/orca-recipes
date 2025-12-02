"""GENIE BPC ELT pipeline"""

import argparse
import os

import pandas as pd
import synapseclient
import yaml

from snowflake_utils import get_connection, logger, write_to_snowflake


def create_snowflake_resources(
    conn,
    syn: synapseclient.Synapse,
    cohort: str,
    version: str,
    clinical_synid: str,
    cbioportal_synid: str,
    overwrite: bool,
    database: str,
) -> None:
    """Create Snowflake schema and upload clinical + cBioPortal tables for one cohort."""
    schema_name = f"{cohort}_{version}"
    logger.info(f"Creating/using schema: {schema_name}")

    with get_connection(conn) as conn:
        with conn.cursor() as cs:
            cs.execute(f"USE DATABASE {database};")
            cs.execute(
                f"CREATE SCHEMA IF NOT EXISTS {schema_name} WITH MANAGED ACCESS;"
            )
            cs.execute(f"USE SCHEMA {schema_name}")

        upload_clinical_tables(
            conn=conn, syn=syn, clinical_synid=clinical_synid, overwrite=overwrite
        )
        upload_cbioportal_tables(
            conn=conn,
            syn=syn,
            cohort=cohort,
            cbioportal_synid=cbioportal_synid,
            overwrite=overwrite,
        )


def upload_clinical_tables(
    conn,
    syn: synapseclient.Synapse,
    clinical_synid: str,
    overwrite: bool,
) -> None:
    """Upload clinical tables to Snowflake."""
    for clinical_file in syn.getChildren(clinical_synid):
        table_name = clinical_file["name"].replace(".csv", "")
        logger.info(f"Uploading clinical table: {table_name}")
        entity = syn.get(clinical_file["id"])
        df = pd.read_csv(entity.path, sep=",", comment="#", low_memory=False)
        if df.empty:
            logger.warning(f"Skipping empty clinical file: {clinical_file['name']}")
            continue

        write_to_snowflake(
            conn=conn,
            table_df=df,
            table_name=table_name,
            overwrite=overwrite,
            quote_identifiers=False,
        )


def upload_cbioportal_tables(
    conn,
    syn: synapseclient.Synapse,
    cohort: str,
    cbioportal_synid: str,
    overwrite: bool,
) -> None:
    """Upload cBioPortal tables to Snowflake, skipping excluded prefixes."""
    exclude_prefixes = ("gene_panel", "meta", "CNA", "case_lists", "seg", "tmb")

    for cbioportal_file in syn.getChildren(cbioportal_synid):
        raw_name = cbioportal_file["name"]

        # Derive clean table name
        table_name = (
            raw_name.replace("data_", "")
            .replace(".txt", "")
            .replace(f"genie_{cohort}_", "")
            .replace("cna_hg19.seg", "seg")
        )

        if table_name.startswith(exclude_prefixes):
            logger.debug(f"Skipping excluded file: {raw_name}")
            continue

        sep = "\t" if raw_name.endswith(".txt") else ","
        logger.info(f"Uploading cBioPortal table: {table_name}")
        entity = syn.get(cbioportal_file["id"])
        df = pd.read_csv(entity.path, sep=sep, comment="#", low_memory=False)

        write_to_snowflake(
            conn=conn,
            table_df=df,
            table_name=table_name,
            overwrite=overwrite,
            quote_identifiers=False,
        )


def main(
    database: str,
    overwrite: bool,
    conn: "snowflake.connector.SnowflakeConnection" = None,
):
    """Main entrypoint for GENIE BPC ELT pipeline."""
    syn = synapseclient.login()
    script_dir = os.path.dirname(__file__)
    yaml_path = os.path.join(script_dir, "genie_bpc_releases.yaml")
    logger.info("Connected to Snowflake.")

    with open(yaml_path, "r") as f:
        cohorts = yaml.safe_load(f)

    for cohort_info in cohorts:
        cohort = cohort_info["cohort"]
        version = cohort_info["version"]
        clinical_synid = cohort_info["clinical_synid"]
        cbioportal_synid = cohort_info["cbioportal_synid"]

        logger.info(f"Processing cohort: {cohort} (version {version})")
        create_snowflake_resources(
            conn=conn,
            syn=syn,
            cohort=cohort,
            version=version,
            clinical_synid=clinical_synid,
            cbioportal_synid=cbioportal_synid,
            overwrite=overwrite,
            database=database,
        )

    logger.info("All cohorts processed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the GENIE BPC ELT pipeline.")
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

"""GENIE BPC ELT pipeline"""

import argparse
import os

import pandas as pd
import synapseclient
import yaml

from snowflake_utils import get_connection, logger, write_to_snowflake


def get_table_name(release_file_key: str, cohort: str) -> str:
    """
    Derive a clean Snowflake table name from a cbioportal/GENIE file name.

    Examples:
      data_clinical_patient.txt -> clinical_patient
      genie_bpc_data_clinical_patient.txt -> clinical_patient
      genie_bpc_cna_hg19.seg -> seg

    Args:
        release_file_key (str): the raw string of the release file
        cohort (str): the name of the cohort this file is from

    Returns:
        str: cleaned and formatted name for the Snowflake table
    """
    table_name = (
        release_file_key.replace("data_", "")
        .replace(".txt", "")
        .replace(f"genie_{cohort}_", "")
        .replace("cna_hg19.seg", "seg")
    )
    return table_name


def create_snowflake_resources(
    conn: "snowflake.connector.SnowflakeConnection",
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
    conn: "snowflake.connector.SnowflakeConnection",
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
    conn: "snowflake.connector.SnowflakeConnection",
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
    """
        Main function - loops through the
        cohorts in the yaml, gets the metadata 
        information such as synapse ids of the folders 
        containing the files to ingest. 
        For BPC there are two for each cohort:
            - clinical files folder
            - cbioportal files folder

    Args:
        overwrite (bool): Whether to overwrite table data or not
        database (str): Database table to run ELT commands in
        conn (snowflake.connector.SnowflakeConnection): Optional Snowflake
            connection injected externally (e.g. Airflow)
    """
    syn = synapseclient.login()
    script_dir = os.path.dirname(__file__)
    yaml_path = os.path.join(script_dir, "genie_bpc_releases.yaml")
    logger.info("Connected to Snowflake.")

    with open(yaml_path, "r") as f:
        cohorts = yaml.safe_load(f)

    conn_obj = get_connection(conn=conn)
    try:
        for cohort_info in cohorts:
            cohort = cohort_info["cohort"]
            version = cohort_info["version"]
            clinical_synid = cohort_info["clinical_synid"]
            cbioportal_synid = cohort_info["cbioportal_synid"]

            logger.info(f"Processing cohort: {cohort} (version {version})")
            create_snowflake_resources(
                conn=conn_obj,
                syn=syn,
                cohort=cohort,
                version=version,
                clinical_synid=clinical_synid,
                cbioportal_synid=cbioportal_synid,
                overwrite=overwrite,
                database=database,
            )

        logger.info("All cohorts processed successfully.")
    finally:
        # if it's a local connection, close it
        if conn is None:
            conn_obj.close()


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

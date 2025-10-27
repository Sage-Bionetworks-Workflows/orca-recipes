"""GENIE ELT pipeline"""

import argparse
import os

from dotenv import dotenv_values
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import synapseclient
import yaml

import genie_utils

log = genie_utils.create_logger()


def _read_synapse_table(
    syn: synapseclient.Synapse, synid: str, sep: str = ","
) -> pd.DataFrame:
    """Fetch a Synapse file entity and load it as a DataFrame."""
    entity = syn.get(synid)
    try:
        df = pd.read_csv(entity.path, sep=sep, comment="#", low_memory=False)
    except pd.errors.EmptyDataError:
        log.warning(f"{entity.path} is empty — skipping.")
        return pd.DataFrame()
    return df


def create_snowflake_resources(
    conn,
    syn: synapseclient.Synapse,
    cohort: str,
    version: str,
    clinical_synid: str,
    cbioportal_synid: str,
    overwrite: bool,
) -> None:
    """Create Snowflake schema and upload clinical + cbioportal tables for one cohort."""
    schema_name = f"{cohort}_{version}"
    log.info(f"Creating/using schema: {schema_name}")

    with conn.cursor() as cs:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name} WITH MANAGED ACCESS;")
        cs.execute(f"USE SCHEMA {schema_name}")

    # --- Clinical tables ---
    for clinical_file in syn.getChildren(clinical_synid):
        table_name = clinical_file["name"].replace(".csv", "")
        df = _read_synapse_table(syn, clinical_file["id"], sep=",")
        genie_utils.write_to_snowflake(
            conn=conn, table_df=df, table_name=table_name, overwrite=overwrite
        )

    # --- cBioPortal tables ---
    exclude_prefixes = ("gene_panel", "meta", "CNA", "case_lists", "seg", "tmb")

    for cbioportal_file in syn.getChildren(cbioportal_synid):
        raw_name = cbioportal_file["name"]
        table_name = (
            raw_name.replace("data_", "")
            .replace(".txt", "")
            .replace(f"genie_{cohort}_", "")
            .replace("cna_hg19.seg", "seg")
        )
        if table_name.startswith(exclude_prefixes):
            log.debug(f"Skipping excluded file: {raw_name}")
            continue

        sep = "\t" if raw_name.endswith(".txt") else ","
        df = _read_synapse_table(syn, cbioportal_file["id"], sep=sep)
        genie_utils.write_to_snowflake(
            conn=conn, table_df=df, table_name=table_name, overwrite=overwrite
        )


def main(args):
    """Main entrypoint for GENIE ELT pipeline."""
    syn = synapseclient.login()
    config = dotenv_values("../.env")

    user = config.get("user")
    password = config.get("password")
    account = config.get("snowflake_account")

    if not all([user, password, account]):
        raise EnvironmentError("Missing Snowflake credentials in .env file")

    with snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database="genie",
        role="SYSADMIN",
        warehouse="compute_xsmall",
    ) as conn:

        log.info("Connected to Snowflake.")

        with open("genie_bpc_releases.yaml", "r") as f:
            cohorts = yaml.safe_load(f)

        for cohort_info in cohorts:
            cohort = cohort_info["cohort"]
            version = cohort_info["version"]
            clinical_synid = cohort_info["clinical_synid"]
            cbioportal_synid = cohort_info["cbioportal_synid"]

            log.info(f"Processing cohort: {cohort} (version {version})")
            create_snowflake_resources(
                conn=conn,
                syn=syn,
                cohort=cohort,
                version=version,
                clinical_synid=clinical_synid,
                cbioportal_synid=cbioportal_synid,
                overwrite=args.overwrite,
            )

        log.info("All cohorts processed successfully.")


def create_snowflake_resources(
    conn: snowflake.connector.connect,
    syn: synapseclient.Synapse,
    cohort: str,
    version: str,
    clinical_synid: str,
    cbioportal_synid: str,
):
    cs = conn.cursor()
    clinical_files = syn.getChildren(clinical_synid)
    cbioportal_files = syn.getChildren(cbioportal_synid)
    schema_name = f"{cohort}_{version}"
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name} WITH MANAGED ACCESS;")
    cs.execute(f"USE SCHEMA {schema_name}")

    write_panda_common_params = {
        "conn": ctx,
        "auto_create_table": True,
        "quote_identifiers": False,
        "overwrite": True,
    }

    for clinical_file in clinical_files:
        print(clinical_file["name"])
        table_name = clinical_file["name"].replace(".csv", "")
        clinical_entity = syn.get(clinical_file["id"])
        clin_df = pd.read_csv(clinical_entity.path, comment="#", low_memory=False)
        write_pandas(df=clin_df, table_name=table_name, **write_panda_common_params)
        genie_utils.write_to_snowflake(
            conn=conn, table_df=table_df, table_name=table_name, overwrite=overwrite
        )

    for cbioportal_file in cbioportal_files:
        # print(cbioportal_file['name'])
        table_name = (
            cbioportal_file["name"]
            .replace("data_", "")
            .replace(".txt", "")
            .replace(f"genie_{cohort}_", "")
            .replace("cna_hg19.seg", "seg")
        )
        # TODO: error when uploading SEG file and CNA file
        exclude = ("gene_panel", "meta", "CNA", "case_lists", "seg", "tmb")
        if not table_name.startswith(exclude):
            print(table_name)
            table_ent = syn.get(cbioportal_file["id"])
            table_df = pd.read_csv(
                table_ent.path, sep="\t", comment="#", low_memory=False
            )
            genie_utils.write_to_snowflake(
                conn=conn, table_df=table_df, table_name=table_name, overwrite=overwrite
            )


def main(args):
    """Main entrypoint for GENIE ELT pipeline."""
    syn = synapseclient.login()
    config = dotenv_values("../.env")

    with snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["snowflake_account"],
        database="genie",
        role="SYSADMIN",
        warehouse="compute_xsmall",
    ) as conn:

        log.info("Connected to Snowflake.")

        with open("genie_bpc_releases.yaml", "r") as f:
            cohorts = yaml.safe_load(f)

        for cohort_info in cohorts:
            cohort = cohort_info["cohort"]
            version = cohort_info["version"]
            clinical_synid = cohort_info["clinical_synid"]
            cbioportal_synid = cohort_info["cbioportal_synid"]

            log.info(f"Processing cohort: {cohort} (version {version})")
            create_snowflake_resources(
                conn=conn,
                syn=syn,
                cohort=cohort,
                version=version,
                clinical_synid=clinical_synid,
                cbioportal_synid=cbioportal_synid,
                overwrite=args.overwrite,
            )

        log.info("All cohorts processed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the GENIE BPC ELT pipeline.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite table data if present.",
    )
    args = parser.parse_args()
    main(args)

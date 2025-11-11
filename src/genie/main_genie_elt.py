"""MAIN GENIE ELT pipeline"""
import argparse
from typing import Dict
import pandas as pd
import synapseclient
import synapseutils as synu

from snowflake_utils import get_connection, logger, write_to_snowflake

STRUCTURED_DATA = (
    "data_clinical",
    "data_mutations",
    "data_fusions",
    "assay_information",
    "data_cna_hg19",
    "data_gene_matrix",
    "data_sv",
    "genomic_information",
    "genie_combined",
    "genie_cna_hg19",
)

PROJECT_SYNID = "syn7492881"


def get_table_schema_name(syn: synapseclient.Synapse, synid: str) -> str:
    """Construct the Snowflake schema name from a Synapse folder name.

    Args:
        syn (synapseclient.Synapse): Synapse client connection
        synid (str): Synapse id of 

    Returns:
        str: constructed snowflake schema name of the format:
            {release_type}_{release_name}
            
            e.g: consortium_19_3
            where release_type can only be "consortium" or "public"
            and release name is "02_1", "19_3"
    """
    folder_ent = syn.get(synid)
    release_name_meta = folder_ent.name.split("-")
    if len(release_name_meta) < 2:
        raise ValueError(f"Unexpected folder name format: {folder_ent.name}")

    release_name = release_name_meta[0].replace(".", "_")
    release_type = release_name_meta[1]
    return f"{release_type}_{release_name}"


def get_table_name(release_file_key : str) -> str:
    """Standardize a file name into a clean Snowflake table name.

    Args:
        release_file_key (str): String of the release file name

    Returns:
        str: standardized release file name to use as the table name
        
        Example) data_clinical_sample.txt -> clinical_sample
    """
    tbl_name = (
        release_file_key.replace("data_", "")
        .replace(".txt", "")
        .replace(".seg", "")
    )
    if tbl_name == "genie_combined.bed":
        tbl_name = "genomic_information"
    elif tbl_name == "genie_cna_hg19.seg":
        tbl_name = "cna_hg19"

    logger.debug(f"Constructed table name: {tbl_name}")
    return tbl_name


def get_cbio_file_map(syn : synapseclient.Synapse, synid : str) -> Dict[str, synapseclient.Entity]:
    """Return a mapping of structured GENIE data file names to Synapse entities.
 
    Args:
        syn (synapseclient.Synapse): synapse client connection
        synid (str): Synapse id of the folder entity to search for files
 
    Returns:
        Dict[str, synapseclient.Entity]: dictionary of the release file name linked to
            its synapse file entity
    """
    release_files = syn.getChildren(synid, includeTypes=["file", "folder", "link"])
    release_file_map = {
        release_file["name"]: syn.get(release_file["id"], followLink=True)
        for release_file in release_files
        if release_file["name"].startswith(STRUCTURED_DATA)
        and release_file["name"].endswith(("txt", "bed"))
    }
    return release_file_map


def push_cbio_files_to_snowflake(syn: synapseclient.Synapse,
    conn: "snowflake.connector.SnowflakeConnection",
    synid: str,
    overwrite : bool,
) -> None:
    """Fetch cbioportal files from Synapse and push them into Snowflake.

    Args:
        syn (synapseclient.Synapse): synapse client connection
        conn (snowflake.connector.connect): Snowflake connection
        synid (str): Synapse id of the release folder
        overwrite (bool): Whether to overwrite table data or not
     """
    release_schema_name = get_table_schema_name(syn=syn, synid=synid)
    release_file_map = get_cbio_file_map(syn=syn, synid=synid)

    with conn.cursor() as cs:
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {release_schema_name} WITH MANAGED ACCESS;")
        cs.execute(f"USE SCHEMA {release_schema_name}")
        logger.info(f"Using schema: {release_schema_name}")

    for release_file_key, release_file_ent in release_file_map.items():
        table_name = get_table_name(release_file_key)
        try:
            df = pd.read_csv(release_file_ent.path, sep="\t", comment="#", low_memory=False)
        except pd.errors.EmptyDataError:
            logger.warning(f"{release_file_ent.path} is empty — skipping.")
            continue
        except Exception as e:
            logger.error(f"Failed to read {release_file_ent.path}: {e}")
            continue

        write_to_snowflake(conn=conn, table_df=df, table_name=table_name, overwrite=overwrite)


def main(args):
    syn = synapseclient.login()

    with get_connection() as conn:
        logger.info("Connected to Snowflake.")
        for dirpath, dirnames, _ in synu.walk(syn, PROJECT_SYNID):
            if len(dirpath[0].split("/")) != 2:
                continue
            for dirname, dir_synid in dirnames:
                logger.info(f"Processing release: {dirname}")
                push_cbio_files_to_snowflake(
                    syn=syn, conn=conn, synid=dir_synid, overwrite=args.overwrite
                )
        logger.info("ELT completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Main GENIE ELT pipeline.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite table data if present.")
    args = parser.parse_args()
    main(args)

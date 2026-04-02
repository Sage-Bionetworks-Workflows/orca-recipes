"""DOI to data catalog pipeline"""

import argparse
import re
from datetime import datetime, timezone
from typing import Dict, NamedTuple


import pandas as pd
# from genie.main_genie_ingestion import parse_release_folder
from snowflake.connector.pandas_tools import write_pandas
import synapseclient
import synapseutils as synu

from snowflake_utils import get_connection, logger
from datacite import fetch_doi_prefix




def extract_public_dois(
    conn: "snowflake.connector.SnowflakeConnection",
) -> pd.DataFrame:
    """
    Extract public DOIS
    """
    query = """
        select
            node_latest.name,
            node_latest.node_type,
            node_latest.annotations:annotations as annotations,
            doi.object_id as node_id,
            doi.object_version as node_version,
            case 
                when doi.object_version != -1 
                    then '10.7303/syn' || doi.object_id || '.' || doi.object_version
                else '10.7303/syn' || doi.object_id
            end as doi_id,
            'https://doi.org/' || doi_id as doi_link,
            node_revision.description
        from
            synapse_rds_snapshot.prod_576.doi
        left join
            synapse_data_warehouse.synapse.node_latest on
            doi.object_id = node_latest.id
        join
            synapse_rds_snapshot.prod_576.node_revision on
            node_latest.id = node_revision.owner_node_id and
            (
                doi.object_version = -1 
                or node_revision.number = doi.object_version
            )
        where
            doi.object_type = 'ENTITY' and
            doi.doi_status = 'READY' and
            node_latest.is_public and
            node_latest.node_type not in ('file', 'table', 'dockerrepo');
    """
    with conn.cursor() as cs:
        cs.execute(
            query
        )
        dois_df = cs.fetch_pandas_all()
        dois_df.drop_duplicates(inplace=True)
    return dois_df



def main(conn=None) -> None:
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

        dois_df = extract_public_dois(
            conn=conn_obj
        )
        dois_df.to_csv("public_dois.csv", index=False)
        logger.info("Ingestion completed successfully.")

        dois = fetch_doi_prefix(prefixes=["10.7303"], state="findable")
        # doi_metadata_df = pd.DataFrame(list(dois))
        doi_metadata_df = pd.DataFrame([i['attributes'] for i in dois])
        doi_metadata_df.to_csv("doi_metadata.csv", index=False)
    
        dois_df.merge(doi_metadata_df[['doi', 'creators', 'descriptions', 'contributors']], left_on="DOI_ID", right_on="doi", how="left").to_csv("dois_with_metadata.csv", index=False)
    finally:
        # if it's a local connection, close it
        if conn is None:
            conn_obj.close()


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Run MAIN GENIE ingestion pipeline.")
    # parser.add_argument(
    #     "--database",
    #     type=str,
    #     default="GENIE_DEV",
    #     choices=["GENIE", "GENIE_DEV"],
    #     help="Database to run ingestion commands in.",
    # )
    # parser.add_argument(
    #     "--overwrite-partition",
    #     action="store_true",
    #     help="Delete existing rows for a RELEASE before appending (recommended for idempotent reruns).",
    # )
    # args = parser.parse_args()
    main()

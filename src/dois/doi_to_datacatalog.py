"""DOI to data catalog pipeline"""

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Dict, NamedTuple


import pandas as pd
# from genie.main_genie_ingestion import parse_release_folder
from snowflake.connector.pandas_tools import write_pandas
import synapseclient
import synapseutils as synu
from synapseclient.models import Table, SchemaStorageStrategy

from snowflake_utils import get_connection, logger
from datacite import fetch_doi_prefix




def extract_public_dois(
    conn: "snowflake.connector.SnowflakeConnection",
) -> pd.DataFrame:
    """
    Extract public DOIS
    """
    query = """
with

-- ============================================================
-- 1. ANCHOR: resolve DOI entities we care about
-- ============================================================
doi_entities as (
    select
        doi.object_id                                           as node_id,
        doi.object_version                                      as node_version,
        case
            when doi.object_version != -1
                then '10.7303/syn' || doi.object_id || '.' || doi.object_version
            else '10.7303/syn' || doi.object_id
        end                                                     as doi_id,
        'https://doi.org/' || doi_id as doi_link,
        node_latest.name,
        node_latest.node_type,
        node_latest.scope_ids,
        node_revision.description,
        node_latest.annotations:annotations as annotations,
    from synapse_rds_snapshot.prod_576.doi
    left join synapse_data_warehouse.synapse.node_latest
        on doi.object_id = node_latest.id
    join synapse_rds_snapshot.prod_576.node_revision
        on node_latest.id = node_revision.owner_node_id
        and (
            doi.object_version = -1
            or node_revision.number = doi.object_version
        )
    where
        doi.object_type = 'ENTITY'
        and doi.doi_status = 'READY'
        and node_latest.is_public
        and node_latest.node_type in ('project', 'folder', 'dataset', 'datasetcollection')
),

-- ============================================================
-- 2. FOLDER: recurse descendants, carrying file_handle_id
--    and node_type at each level
-- ============================================================
folder_descendants as (
    select
        node_id::bigint                                         as root_id,
        node_id::bigint                                         as descendant_id,
        null::bigint                                            as file_handle_id,
        node_type::varchar                                      as node_type
    from doi_entities
    where node_type = 'folder'

    union all

    select
        fd.root_id,
        child.id::bigint                                        as descendant_id,
        child.file_handle_id::bigint,
        child.node_type::varchar
    from folder_descendants fd
    join synapse_data_warehouse.synapse.node_latest child
        on child.parent_id = fd.descendant_id
),

folder_files as (
    select
        root_id,
        descendant_id                                           as file_node_id,
        file_handle_id
    from folder_descendants
    where node_type = 'file'
),

-- ============================================================
-- 3. DATASET: explode scope_ids -> look up file_handle_id
-- ============================================================
dataset_scope_ids as (
    select
        de.node_id                                              as dataset_id,
        try_cast(s.value::string as bigint)                     as file_node_id
    from doi_entities de,
        lateral flatten(input => parse_json(de.scope_ids)) s
    where de.node_type = 'dataset'
),

dataset_files as (
    select
        ds.dataset_id,
        nl.id                                                   as file_node_id,
        nl.file_handle_id
    from dataset_scope_ids ds
    join synapse_data_warehouse.synapse.node_latest nl
        on nl.id = ds.file_node_id
),

-- ============================================================
-- 4. DATASETCOLLECTION: scope_ids -> dataset IDs ->
--    each dataset's scope_ids -> file members
-- ============================================================
collection_dataset_ids as (
    select
        de.node_id                                              as collection_id,
        try_cast(s.value::string as bigint)                     as dataset_id
    from doi_entities de,
        lateral flatten(input => parse_json(de.scope_ids)) s
    where de.node_type = 'datasetcollection'
),

collection_files as (
    select
        cd.collection_id,
        nl_file.id                                              as file_node_id,
        nl_file.file_handle_id
    from collection_dataset_ids cd
    join synapse_data_warehouse.synapse.node_latest nl_dataset
        on nl_dataset.id = cd.dataset_id,
        lateral flatten(input => parse_json(nl_dataset.scope_ids)) f
    join synapse_data_warehouse.synapse.node_latest nl_file
        on nl_file.id = try_cast(f.value::string as bigint)
),

-- ============================================================
-- 5. AGGREGATE unique downloaders in the last year
-- ============================================================
project_downloads as (
    select
        e.project_id                                            as node_id,
        count(distinct e.user_id)                               as unique_downloaders
    from synapse_data_warehouse.synapse_event.objectdownload_event e
    where e.project_id in (
        select node_id from doi_entities where node_type = 'project'
    )
    and e.record_date >= dateadd(year, -1, current_date)
    group by e.project_id
),

folder_downloads as (
    select
        ff.root_id                                              as node_id,
        count(distinct e.user_id)                               as unique_downloaders
    from folder_files ff
    join synapse_data_warehouse.synapse_event.objectdownload_event e
        on e.association_object_id = ff.file_node_id
        and e.file_handle_id = ff.file_handle_id
    where e.record_date >= dateadd(year, -1, current_date)
    group by ff.root_id
),

dataset_downloads as (
    select
        df.dataset_id                                           as node_id,
        count(distinct e.user_id)                               as unique_downloaders
    from dataset_files df
    join synapse_data_warehouse.synapse_event.objectdownload_event e
        on e.file_handle_id = df.file_handle_id
        and e.association_object_id = df.file_node_id
    where e.record_date >= dateadd(year, -1, current_date)
    group by df.dataset_id
),

collection_downloads as (
    select
        cf.collection_id                                        as node_id,
        count(distinct e.user_id)                               as unique_downloaders
    from collection_files cf
    join synapse_data_warehouse.synapse_event.objectdownload_event e
        on e.file_handle_id = cf.file_handle_id
        and e.association_object_id = cf.file_node_id
    where e.record_date >= dateadd(year, -1, current_date)
    group by cf.collection_id
)

-- ============================================================
-- 6. FINAL OUTPUT
-- ============================================================
select
    de.name,
    de.node_type,
    de.node_id,
    -- de.node_version,
    de.doi_link,
    de.doi_id,
    de.description,
    de.annotations,
    coalesce(
        case de.node_type
            when 'project'           then pd.unique_downloaders
            when 'folder'            then fold.unique_downloaders
            when 'dataset'           then dd.unique_downloaders
            when 'datasetcollection' then cd.unique_downloaders
        end,
        0
    )                                                           as unique_downloaders_12_months
from doi_entities de
left join project_downloads pd
    on de.node_type = 'project'
    and de.node_id = pd.node_id
left join folder_downloads fold
    on de.node_type = 'folder'
    and de.node_id = fold.node_id
left join dataset_downloads dd
    on de.node_type = 'dataset'
    and de.node_id = dd.node_id
left join collection_downloads cd
    on de.node_type = 'datasetcollection'
    and de.node_id = cd.node_id
order by unique_downloaders_12_months desc nulls last
;
    """
    with conn.cursor() as cs:
        cs.execute(
            query
        )
        dois_df = cs.fetch_pandas_all()
        dois_df.drop_duplicates(inplace=True)
    dois_df["ANNOTATIONS"] = dois_df["ANNOTATIONS"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else x
    )
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
        logger.info("Extract Public DOIs completed successfully.")

        dois = fetch_doi_prefix(prefixes=["10.7303"], state="findable")
        logger.info("Fetched all Public datacite data")

        # doi_metadata_df = pd.DataFrame(list(dois))
        doi_metadata_df = pd.DataFrame([i['attributes'] for i in dois])
        doi_metadata_df.drop_duplicates('doi', inplace=True)
        doi_metadata_df.to_csv("doi_metadata.csv", index=False)
    
        dois_df = dois_df.merge(doi_metadata_df[['doi', 'creators', 'descriptions', 'contributors', 'subjects', 'titles']], left_on="DOI_ID", right_on="doi", how="left")
        dois_df.rename(columns={
            "creators": "datacite_creators",
            "descriptions": "datacite_descriptions",
            "contributors": "datacite_contributors",
            "subjects": "datacite_subjects",
            "titles": "datacite_titles",
        }, inplace=True)
        dois_df.drop(columns=["doi"], inplace=True)
        dois_df.to_csv("dois_with_metadata.tsv", index=False, sep="\t")
        logger.info("Store data initially")
        # data catalog staging table
        table = Table(
            id="syn74257215",
        )
        table.upsert_rows(values=dois_df,  primary_keys=['DOI_ID'])
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

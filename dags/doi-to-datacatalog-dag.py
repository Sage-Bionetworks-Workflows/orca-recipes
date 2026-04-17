"""Ingest public Synapse entities with DOIs into the Synapse data catalog.

Pipeline steps:
1. Extract public DOIs from Snowflake and fetch DataCite metadata (in parallel).
2. Merge Synapse and DataCite metadata.
3. Submit Claude batch for enrichment, then wait via sensor (reschedule mode).
4. Collect enrichment results, filter, and upsert into the data catalog.
"""

import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from src.anthropic_batch_sensor import AnthropicBatchSensor
from src.anthropic_hook import AnthropicHook
from src.datacite import fetch_doi_prefix
from src.dois.doi_to_datacatalog import (
    collect_enrichment_results,
    extract_public_dois,
    load_to_data_catalog,
    merge_doi_metadata,
    submit_enrichment_batch,
    transform_datacite_dois,
    transform_synapse_dois,
)
from src.synapse_hook import SynapseHook

logger = logging.getLogger(__name__)

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "anthropic_conn_id": Param("ANTHROPIC_CONN", type="string"),
    "data_catalog_table_id": Param("syn74257215", type="string"),
    "existing_datasets_table_id": Param("syn61609402", type="string"),
}

dag_config = {
    "schedule": "0 0 1 * *",
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake", "synapse", "data-catalog"],
    "params": dag_params,
}


def _tmp_path(prefix: str, ts_nodash: str) -> str:
    return f"/tmp/doi_{prefix}_{ts_nodash}.pkl"


@dag(**dag_config)
def doi_to_datacatalog() -> None:
    """Sync public Synapse DOI entities into the Synapse data catalog table."""

    @task
    def extract_synapse_dois(**context) -> str:
        """Query Snowflake for public DOI entities with sizes and download counts."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        conn = snow_hook.get_conn()
        try:
            dois_df = extract_public_dois(conn=conn)
        finally:
            conn.close()

        synapse_df = transform_synapse_dois(dois_df)
        logger.info("Extracted %d public DOIs from Snowflake.", len(synapse_df))

        path = _tmp_path("synapse", context["ts_nodash"])
        synapse_df.to_pickle(path)
        return path

    @task
    def fetch_datacite_dois(**context) -> str:
        """Fetch all findable DOIs with prefix 10.7303 from the DataCite API."""
        datacite_df = transform_datacite_dois(
            fetch_doi_prefix(prefixes=["10.7303"], state="findable")
        )
        logger.info("Fetched %d DOIs from DataCite.", len(datacite_df))

        path = _tmp_path("datacite", context["ts_nodash"])
        datacite_df.to_pickle(path)
        return path

    @task
    def merge(synapse_path: str, datacite_path: str, **context) -> str:
        """Join Synapse DOI data with DataCite metadata."""
        merged_df = merge_doi_metadata(
            pd.read_pickle(synapse_path),
            pd.read_pickle(datacite_path),
        )
        logger.info("Merged metadata: %d rows.", len(merged_df))

        path = _tmp_path("merged", context["ts_nodash"])
        merged_df.to_pickle(path)
        return path

    @task
    def submit_batch(merged_path: str, **context) -> str:
        """Fetch wikis, build prompts, and submit the Claude batch. Returns batch_id."""
        merged_df = pd.read_pickle(merged_path)

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        anthropic_hook = AnthropicHook(context["params"]["anthropic_conn_id"])
        batch_id = submit_enrichment_batch(merged_df, syn_hook.client, anthropic_hook.client)
        logger.info("Submitted batch: %s", batch_id or "none needed")
        return batch_id

    @task
    def collect_and_load(merged_path: str, batch_id: str, **context) -> None:
        """Collect batch results, apply enrichment, filter, and upsert to Synapse."""
        merged_df = pd.read_pickle(merged_path)

        anthropic_hook = AnthropicHook(context["params"]["anthropic_conn_id"])
        enriched_df = collect_enrichment_results(merged_df, batch_id, anthropic_hook.client)
        logger.info("Enrichment results collected.")

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        n_loaded = load_to_data_catalog(
            df=enriched_df,
            syn=syn_hook.client,
            data_catalog_table_id=context["params"]["data_catalog_table_id"],
            existing_datasets_table_id=context["params"]["existing_datasets_table_id"],
        )
        logger.info("Loaded %d rows into %s.", n_loaded, context["params"]["data_catalog_table_id"])

    # extract_synapse_dois and fetch_datacite_dois run in parallel
    synapse_path = extract_synapse_dois()
    datacite_path = fetch_datacite_dois()
    merged_path = merge(synapse_path, datacite_path)
    batch_id = submit_batch(merged_path)

    wait_for_batch = AnthropicBatchSensor(
        task_id="wait_for_batch",
        batch_id=batch_id,
        anthropic_conn_id="{{ params.anthropic_conn_id }}",
        mode="reschedule",
        poke_interval=60,
    )

    wait_for_batch >> collect_and_load(merged_path, batch_id)


dag = doi_to_datacatalog()

if __name__ == "__main__":
    dag.test()

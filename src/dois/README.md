# DOI to Data Catalog Pipeline

Extracts public Synapse entities with DOIs from Snowflake, enriches them with DataCite metadata and AI-generated descriptions, and upserts the results into a Synapse table.

## Pipeline steps

1. **Extract** — Query Snowflake for all public Synapse entities (`project`, `folder`, `dataset`, `datasetcollection`) that have a registered DOI and are not already in the data catalog table.
2. **Enrich descriptions** — For any entity missing a description, call Claude via the Synapse MCP server to look up the entity and generate a short description.
3. **Fetch DataCite metadata** — Pull creator, title, license, and subject metadata from the DataCite API for the `10.7303` prefix.
4. **Merge** — Join Synapse and DataCite data into a single catalog record per DOI.
5. **Load** — Upsert new records into the Synapse data catalog table (`syn74257215`), keyed on `DOI_ID`.

## Requirements

### Python packages

```
anthropic>=0.52.0
pandas<3.0.0
synapseclient
```

Install the module and its dependencies:

```bash
pip install -e src/dois/
```

The `datacite` and `snowflake-utils` local packages are also required:

```bash
pip install -e src/datacite/
pip install -e src/snowflake_utils/
```

### Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Anthropic API key. Used to call Claude for description enrichment. |
| `SYNAPSE_AUTH_TOKEN` | Yes | Synapse personal access token. Used by `synapseclient` to authenticate and fetch entity wikis. |
| `SNOWFLAKE_*` | Yes | Snowflake connection vars consumed by `snowflake_utils`. See `src/snowflake_utils/` for details. |

Set them before running:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
export SYNAPSE_AUTH_TOKEN="your-synapse-pat"
```

### External services

| Service | Purpose |
|---------|---------|
| Snowflake (`synapse_data_warehouse`, `synapse_rds_snapshot`) | Source of DOI and node metadata |
| DataCite API (`api.datacite.org`) | DOI metadata (creators, titles, licenses) |
| Synapse REST API | Entity wiki lookup for description enrichment |
| Synapse table `syn74257215` | Destination — data catalog |
| Synapse table `syn61609402` | Existing IDs — used to skip already-ingested records |

## Usage

```bash
cd src/dois
python doi_to_datacatalog.py
```

Intermediate output is written to `dois_with_metadata.tsv` before the Synapse upsert.

## Description enrichment

`enrich_descriptions()` is called after the Snowflake extract. It iterates over rows where `DESCRIPTION` is blank and calls the Anthropic API with the Synapse MCP server attached. Claude uses the MCP tools (`get_entity`, `search_synapse`, etc.) to look up the entity by its `syn{node_id}` ID and returns a 1-2 sentence plain-text description.

Rows that already have a description are skipped, so this step is cheap on incremental runs.

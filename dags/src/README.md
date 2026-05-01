# dags/src

This directory contains the business logic for DAGs, separated from Airflow DAG definitions. Keeping logic here makes it independently testable without an Airflow runtime.

DAG files in `dags/` should be thin orchestration layers that import from here.

## Contents

| Module | Description |
|---|---|
| `datacite/datacite.py` | Fetches DOI metadata from the DataCite REST API, handles pagination and retries, and writes to gzipped NDJSON |
| `dois/doi_to_datacatalog.py` | Pipeline logic for extracting public DOIs from Snowflake and loading them into the Synapse data catalog |
| `anthropic_hook.py` | Airflow hook for the Anthropic API — resolves the API key from an Airflow connection or env var |
| `anthropic_batch_sensor.py` | Airflow sensor that polls an Anthropic message batch until it reaches `ended` status |
| `synapse_hook.py` | Airflow hook for Synapse — resolves credentials from an Airflow connection or env var |

## Retrieving the Anthropic API Key

`AnthropicHook` resolves the API key in two ways, tried in order:

1. **Airflow connection (production)** — create a connection in the Airflow UI with the connection ID you pass to `AnthropicHook(conn_id=...)`. Set the **Password** field to your Anthropic API key. The connection type can be set to "Generic".

2. **Environment variable (local/fallback)** — if the Airflow connection can't be resolved (e.g. running locally outside Airflow), the hook falls back to the `ANTHROPIC_API_KEY` environment variable.

### Setting up locally

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

### Setting up in Airflow

In the Airflow UI: **Admin → Connections → +**

| Field | Value |
|---|---|
| Connection Id | `anthropic_default` (or whatever you pass as `conn_id`) |
| Connection Type | Generic |
| Password | `sk-ant-...` |

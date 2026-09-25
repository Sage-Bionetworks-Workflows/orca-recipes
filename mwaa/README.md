# MWAA DAG Development Guide

Amazon Managed Workflows for Apache Airflow (MWAA) has several constraints and conventions that differ from local Airflow. This document covers what you need to know before writing or deploying a DAG.

## Requirements and Dependency Constraints

Dependencies are declared in `mwaa/requirements.txt`. The first line pins a constraints file from the official Airflow release:

```
-c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt
```

All packages must be compatible with this constraints file. MWAA enforces this at environment startup — a conflicting package will prevent the environment from loading entirely.

**Known quirks:**
- `synapseclient[pandas]~=4.0` installs successfully even though the constraints file pins `urllib3` to a version that appears incompatible. MWAA resolves this at install time; do not add an explicit `urllib3` pin to work around it.
- Stick to packages already listed or commented out in `requirements.txt` as a reference — they have been tested against the constraints file.
- Before adding a new package, verify it does not conflict with pinned transitive dependencies in the constraints file.

## Syncing DAGs to S3

MWAA reads DAGs directly from an S3 bucket. The sync step is not yet automated — DAG files must be uploaded manually to the designated S3 bucket in the AWS account. The bucket path and sync process are to be documented once set up.

DAG files live under `mwaa/dags/`. Upload the entire `dags/` directory (including the `src/` subdirectory — see below) to the S3 bucket prefix that MWAA is configured to watch.

## Business Logic in `src/`

Shared Python modules can be placed in `mwaa/dags/src/`. MWAA picks up this directory alongside the DAG files, so business logic placed there is importable inside tasks:

```python
from src.synapse_hook import SynapseHook
```

Keeping logic in `src/` rather than inline in DAG files has two benefits:
1. It can be unit-tested independently of Airflow.
2. It is reusable across multiple DAGs.

Always include an `__init__.py` in any new subdirectory under `src/`.

## Serialization: No Dataclasses

MWAA uses Airflow's DAG serialization, which serializes task return values to the XCom backend as JSON. Python `dataclasses` (and any class relying on `pickle`-based serialization) do not survive this round-trip.

**Rule:** task functions must accept and return only JSON-serializable types — `dict`, `list`, `str`, `int`, `float`, `bool`, or `None`. Do not use `dataclasses`, `NamedTuple`, or any custom class as a task input or output.

Instead of:
```python
@dataclass
class Metrics:
    total_size: float
    active_users: int
```

Use a plain dict:
```python
{"total_size": 1.23, "active_users": 456}
```

## Airflow Connections

Credentials (Snowflake, Synapse, etc.) are stored as Airflow Connections in the MWAA environment via AWS secrets manager. DAG parameters expose the connection ID so it can be overridden at trigger time without changing code. See `synapse_hook.py` for the pattern used to resolve a connection with a fallback to an environment variable for local runs.

## Local Development

1. For local development and testing, use the `@dag.test`
2. Install `pip install apache-airflow==2.10.3`
3. Install `pip install -r requirements.txt`

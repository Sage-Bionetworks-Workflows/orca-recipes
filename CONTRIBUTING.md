# Contribution Guidelines

## Table of Contents

- [Development](#development)
  - [Environment](#environment)
    - [Infrastructure](#infrastructure)
      - [Core Infrastructure Files](#core-infrastructure-files)
      - [Development Environment Files](#development-environment-files)
    - [Code](#code)
  - [Structure](#structure)
  - [Testing](#testing)
    - [Unit Testing](#unit-testing)
      - [Writing tests for a DAG](#writing-tests-for-a-dag)
      - [Validating DAG structure](#validating-dag-structure)
    - [Integration Testing](#integration-testing)
      - [DAG Set Up](#dag-set-up)
      - [DAG Testing](#dag-testing)
    - [Testing DAGs Locally](#testing-dags-locally)
      - [Skip AWS Secrets Manager for Local Development](#skip-aws-secrets-manager-for-local-development)
      - [Handling Airflow Variables Locally](#handling-airflow-variables-locally)
      - [Passing Custom Config with `dag.test()`](#passing-custom-config-with-dagtest)
  - [Linting](#linting)
  - [Pre-commit hooks](#pre-commit-hooks)
- [Deployment Infrastructure](#deployment-infrastructure)
- [DAG Development Best Practices](#dag-development-best-practices)
  - [Communication Between Tasks](#communication-between-tasks)
  - [Code Quality](#code-quality)
  - [Shared Utilities](#shared-utilities)
    - [Logging](#logging)
    - [Validating Required Secrets](#validating-required-secrets)
  - [Testing and Validation](#testing-and-validation)
  - [DAG Failure Alerts](#dag-failure-alerts)
- [Secrets](#secrets)
  - [Creating a New Secret](#creating-a-new-secret)
  - [Configuring a SynapseHook Connection Secret](#configuring-a-synapsehook-connection-secret)
  - [Configuring a SnowflakeHook Connection Secret](#configuring-a-snowflakehook-connection-secret)
- [Contributing a Challenge DAG](#contributing-a-challenge-dag)
  - [TL;DR](#tldr)
  - [Overview](#overview)
  - [Creating Your Challenge DAG Config](#creating-your-challenge-dag-config)
    - [Parameters](#parameters)
  - [Contributing a New Challenge DAG](#contributing-a-new-challenge-dag)
  - [Troubleshooting](#troubleshooting)
    - [Airflow Provider Hooks Failing to Connect — Outdated Codespace Secrets](#airflow-provider-hooks-failing-to-connect--outdated-codespace-secrets)

## Development

The following section is divided into three parts:

1. **Environment** – Development information related to the infrastructure or environment in which DAGs run.
2. **Structure** – Development information related to DAG logic/code.
3. **Testing** – Testing DAGs

### Environment

The development environment breaks down into two categories: Infrastructure and Code. This is because the repo contains both:

* Airflow DAG **code** (the workflows), which need appropriate Python environments.
* Configuration files which construct the services that make up Airflow (the **infrastructure** which these workflows run upon).

#### Infrastructure

The Airflow infrastructure is containerized and orchestrated using Docker Compose for local development. See the [README](./README.md) for instructions on how to set up the development environment. The following files define and configure the Airflow environment:

##### Core Infrastructure Files

* `docker-compose.yaml` - Orchestrates the multi-container Airflow setup, including:
   * Airflow webserver, scheduler, and workers
   * PostgreSQL database (metadata storage)
   * Redis (message broker for CeleryExecutor)
   * Container networking, volumes, and health checks

* `Dockerfile` - Builds the custom Airflow Docker image:
   * All Python DAGs run within this environment

* `config/airflow.cfg` - Airflow configuration file that controls:
   * Scheduler behavior and intervals
   * Executor settings (CeleryExecutor)
   * Secrets backend configuration (AWS Secrets Manager)
   * Logging, security, and other operational settings

##### Development Environment Files

* `.devcontainer/devcontainer.json` - VS Code Dev Container configuration for GitHub Codespaces and local development:
   * Configures the development environment, defines VS Code extensions to install, and sets up port forwarding and environment variables.

* `.env.example` - Template for environment variables used by Docker Compose:
   * Note that this is not necessarily the preferred way to pass runtime configuration settings
   * Can include Airflow connection strings, AWS credentials for secrets backend, etc.

When making changes to infrastructure files (Dockerfile, docker-compose.yaml, config files), you'll need to rebuild the containers to see your changes take effect. (See code example in "Integration Testing" section).

#### Code

Python dependencies are managed in requirement files.

Any python packages needed for DAG tasks or the DAGs themselves belongs in [requirements-airflow.txt](./requirements-airflow.txt).

Any python packages needed for development, including running tests, belongs in [requirements-dev.txt](./requirements-dev.txt).

### Structure

We have structured this repo such that DAG _task_ logic ought to be separate from DAG logic. This makes testing of DAGs as a whole easier, since we can separately test task logic and DAG logic. This breaks down into three directories:

- `src/` - This is where DAG task logic belongs. Code is organized as packages that can be imported by DAGs as needed.
- `dags/` - This is where DAG logic belongs.
- `tests/` - Unit tests for both the DAG task logic (packages in `src/`) and the DAGs themselves (`dags/`) belongs here. See the "Testing" Section below for more information.

There is one additional directory where workflows can be found, although it is not part of the current framework for managing DAGs and their task logic.

- `local/` - (DEPRECATED). Project-specific scripts and utilities.

### Testing

Testing breaks down into two categories: formal testing via unit tests and relatively informal testing via integration tests.

#### Unit Testing

Unit tests can be found in `tests/`. We use `pytest` as part of a Github actions workflow to automatically run tests when new commits are pushed to a branch. Tests can also be run locally, provided you are working in the appropriate development environment (See [README.md](./README.md) for instruction on how to set up the dev environment).

```
python -m pytest tests/ -v --tb=short
```

Because of the wide variety of use-cases which this repo supports, we further divide tests into subdirectories within `tests/` depending on their domain. For example, the `tests/datacite/` directory contains tests for everything in the `src/datacite/` directory.

DAG unit tests belong in the `tests/dags/` directory. Unlike DAG task logic, which is much more diverse, DAG logic is homogenous enough that we can organize all DAG unit tests in a single directory.

You are welcome to write tests in any form which `pytest` supports, although it is recommended that you make use of fixtures to keep tests easy to maintain and organize unit tests into classes for ease of testing.

The below directory structure demonstrates a typical way to keep things organized:
```
tests/
├── mypackage/
│   ├── __init__.py           # Package marker
│   ├── conftest.py           # Pytest fixtures (auto-discovered)
│   └── test_mypackage.py     # Test suite
├── dags/
│   ├── __init__.py           # Package marker
│   └── test_mydag.py         # Test suite
```

##### Writing tests for a DAG

The repo's [pytest.ini](./pytest.ini) puts both `src/` and `dags/` on the import
path (`pythonpath = src dags`), so a test can import a DAG module directly along
with any shared hooks or utilities it depends on:

```python
from dags import zenodo_tep_metrics_dag as dag_module   # the DAG under test
```

A few requirements make this import work:

- **The DAG filename must be a valid Python module name** — use underscores, not
  hyphens so it can be imported with `from dags import <dag_module>` (e.g.
  `dags/zenodo_tep_metrics_dag.py`, not `dags/zenodo-tep-metrics-dag.py`)
- **Keep `dags/`, and the top-level `src/` as namespace packages**,
  do not add an `__init__.py` to any of them. Because neither `src/` nor
  `dags/src/` has an `__init__.py`, `import src` resolves to a single namespace
  package spanning both, so a DAG's `from src.synapse_hook import SynapseHook` and
  a test's `from src.datacite... import ...` both resolve. Adding an `__init__.py`
  to `dags/src/` turns it into a regular package that shadows the top-level `src/`
  and breaks the other test suites.

Prefer testing a DAG's **task-logic functions** (the module-level helper
functions) directly rather than the `@dag`/`@task` wiring, and mock any external
calls (HTTP, Synapse, Snowflake) so tests are self-contained and need no credentials.
See [tests/dags/test_zenodo_tep_metrics_dag.py](./tests/dags/test_zenodo_tep_metrics_dag.py)
for a worked example that mocks `requests` for the Zenodo API pull and stubs
`SynapseHook` for the notification logic:

```python
def test_categorize_title():
    assert dag_module.categorize_title("A Useful Component") == "component"

def test_fetch_tep_records(monkeypatch):
    monkeypatch.setattr(dag_module.requests, "get", mock_get)
    records = dag_module.fetch_tep_records(api_token="fake-token")
    assert records == [...]
```

#### Validating DAG structure

As a best practice, add a
`test_dag_structure` test in your DAG's own test module that asserts the exact
task set and the dependency edges you expect. This catches a miswired pipeline
(e.g: a dropped `>>`, a task pointed at the wrong upstream). Load just your DAG file so unrelated DAGs can't interfere:

```python
from airflow.models import DagBag

def test_dag_structure():
    dagbag = DagBag(
        dag_folder="dags/zenodo_tep_metrics_dag.py", include_examples=False
    )
    dag = dagbag.get_dag("zenodo_tep_metrics_dag")

    assert dag is not None, "zenodo_tep_metrics_dag failed to load"
    # Exact task set
    assert {task.task_id for task in dag.tasks} == {
        "fetch_metrics",
        "validate_metrics",
        "export_to_synapse",
        "notify_collaborators",
    }
    # Dependency chain: fetch -> validate -> export -> notify
    assert dag.get_task("fetch_metrics").downstream_task_ids == {"validate_metrics"}
    assert dag.get_task("validate_metrics").downstream_task_ids == {"export_to_synapse"}
    assert dag.get_task("export_to_synapse").downstream_task_ids == {"notify_collaborators"}
```

See [Airflow's Testing a DAG](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag) for detailed information on DAG tests and best practices.

#### Integration Testing

Presently, integration testing means triggering your DAG in Airflow and manually inspecting the results. See the [README.md](README.md) on how to deploy and connect to Airflow.

##### DAG Set Up

Any edits to your DAG should automatically be picked up by the Airflow scheduler/webserver after a short time interval (see `scheduler.min_file_process_interval` in [airflow.cfg](config/airflow.cfg)). New DAGs are picked up by the scheduler/webserver according to a different interval (see `scheduler.dag_dir_list_interval`). You can force a "hard refresh" by restarting the containers:

```console
docker compose restart
```

##### DAG Testing

Integration testing can be performed by triggering a DAG via the Airflow command-line or web UI. Note that for testing of the DAGs directly on Airflow locally via Dev Containers, it's best to leave the DAG **unpaused** when triggering the DAG with various updates, otherwise you might be triggering the DAG twice and/or triggering it in its original state that had its parameters set to production mode.

> [!NOTE]
> Some DAGs use runtime configuration in the form of Params or Connections and Secrets. It's not always well-documented in the DAG itself how the runtime configuration is set up, so if your DAG uses runtime configuration, yet it's not clear how these values are passed through to the DAG itself, it's generally better to test the DAG in GitHub Codespaces.

Logs can be inspected with docker compose:

```console
# All logs
docker compose logs -f

# Logs for a specific service(s)
docker compose ps --services
docker compose logs -f airflow-webserver airflow-scheduler
```

If you edit `Dockerfile`, `docker-compose.yaml`, `requirements-*.txt`, or configuration files, or otherwise want to redo the build process, rebuild the containers:

```console
docker compose down
docker compose up --build --detach
# OR
# do not use cached images
# docker compose up --no-cache --build --detach
```

#### Testing DAGs Locally

There are two distinct ways to test a DAG's task logic without deploying to Airflow:

- **Unit tests** — mock external dependencies (hooks like `NextflowTowerHook`/`SynapseHook`, `WebClient`, `Variable`, etc.) and call each task's `python_callable` directly. These are fast, require no credentials or network access, and run automatically in CI on every push. They validate your Python logic (parameter wiring, branching, string formatting), not whether the real external services behave as you assume. See `tests/` for examples.

- **Local integration runs** via `dag.test()` — hit real external services using real credentials. Credentials can come from AWS Secrets Manager (matching production) or from a local `connections.yaml/AIRFLOW_VAR_*` configuration via `LocalFilesystemBackend`. These validate the real integration (e.g., actually launching a Nextflow Tower workflow or posting to Slack), but are slower, require AWS SSO access, and can have real side effects. Use this as a manual sanity check before/after changing integration behavior, not as an automated substitute for unit tests.

##### Skip AWS Secrets Manager for Local Development

If you'd rather not authenticate to AWS just to test a DAG (e.g., to talk to the real Nextflow Tower), use Airflow's `LocalFilesystemBackend` instead, backed by a local `connections.yaml` file:

```console
export AIRFLOW__SECRETS__BACKEND=airflow.secrets.local_filesystem.LocalFilesystemBackend
export AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_file_path": "connections.yaml"}'
```

Create `connections.yaml` from the template:

```console
cp connections.yaml.example connections.yaml
```

Fill in the real credentials for whichever connections your DAG needs, keyed by your DAG's `tower_conn_id` param value:

```yaml
# Replace this key with your actual connection ID (e.g. AGORA_PROJECT_TOWER_CONN)
AGORA_PROJECT_TOWER_CONN:
  conn_type: tower
  host: tower.sagebionetworks.org
  schema: api
  password: "<your-tower-personal-access-token>"
  extra:
    workspace: sage-bionetworks/<your-workspace>
```

`host`/`schema` become the Tower API endpoint (`https://<host>/<schema>`), `password` is your Tower personal access token, and `extra.workspace` is the fully-qualified `<org>/<workspace>` name.

##### Handling Airflow Variables Locally

`LocalFilesystemBackend`'s `connections_file_path` config above only covers Connections, not Airflow **Variables**. Some DAGs also call `Variable.get("SOME_NAME")` — for example, `SLACK_DPE_TEAM_BOT_TOKEN`, a Slack bot token normally pulled from AWS Secrets Manager. Once you switch away from `SecretsManagerBackend`, there's no source for Variables at all — a plain env var like `SLACK_DPE_TEAM_BOT_TOKEN` in your shell or `.env` is **not** visible to `Variable.get()`. There are two ways to fill that gap without AWS access:

**Option A: `AIRFLOW_VAR_` environment variable**

Prefix the Variable's name with `AIRFLOW_VAR_` and export it as a regular environment variable, e.g. `AIRFLOW_VAR_SLACK_DPE_TEAM_BOT_TOKEN`. Airflow checks for this automatically, regardless of which secrets backend is configured — so it works even with `LocalFilesystemBackend`, which has no Variables source of its own.

```console
export AIRFLOW_VAR_SLACK_DPE_TEAM_BOT_TOKEN="<the-secret-value>"
```

**Option B: `variables_file_path` via `LocalFilesystemBackend`**

Alternatively, point `LocalFilesystemBackend` at a local `variables.json` file the same way you did for `connections.yaml`, by adding `variables_file_path` to the backend kwargs:

```console
export AIRFLOW__SECRETS__BACKEND=airflow.secrets.local_filesystem.LocalFilesystemBackend
export AIRFLOW__SECRETS__BACKEND_KWARGS='{"variables_file_path": "variables.json", "connections_file_path": "connections.yaml"}'
```

Then create `variables.json` with the Variables your DAG needs:

```json
{
  "SLACK_DPE_TEAM_BOT_TOKEN": "<the-secret-value>"
}
```

This keeps all your local secrets (Connections and Variables) sourced the same way, which is handy if a DAG needs several Variables.

##### Passing Custom Config with `dag.test()`

When calling `dag.test()` directly (e.g., from a DAG file's `if __name__ == "__main__":` block), you can override the DAG's default `Param` values for that run via the `run_conf` argument, the same way `-c`/`--conf` works with `airflow dags test <dag_id>` on the CLI:

```python
if __name__ == "__main__":
    dag.test(run_conf={
        "tower_run_name": "my-test-run",
        "profile": "model_ad_preprod",
        "dataset": "model_details",
    })
```

Only keys that match one of the DAG's declared `Param` names actually take effect — anything else in `run_conf` is ignored by your params.

### Linting

We lint the DAGs with [Ruff](https://docs.astral.sh/ruff/) as part of the same
GitHub Actions workflow that runs the tests
([validate.yml](./.github/workflows/validate.yml)). The check uses Ruff's
**`AIR3`** ruleset, which flags Airflow 3.x deprecations and removals (e.g.
`schedule_interval` -> `schedule`, `execution_date`, moved import paths) so DAGs
don't silently break when the Airflow runtime is upgraded.

`ruff` is included in [requirements-dev.txt](./requirements-dev.txt), so once
your dev environment is set up via:

```console
pip install -r requirements-dev.txt
```

you can then run the same check locally before pushing with no separate install needed:

```console
ruff check dags/<your_dag_name> --select AIR3
```

#### Pre-commit hooks

So the same checks run automatically on every commit, the repo ships a
[.pre-commit-config.yaml](./.pre-commit-config.yaml) that wires up Ruff (via
[ruff-pre-commit](https://github.com/astral-sh/ruff-pre-commit), pinned to the
same version and ruleset as Github Actions CI) plus a few standard hygiene hooks
(`check-yaml`, `end-of-file-fixer`, `trailing-whitespace`, etc.).

`pre-commit` is included in [requirements-dev.txt](./requirements-dev.txt). Set
it up once in your dev environment:

```console
pip install pre-commit        # or: pip install -r requirements-dev.txt
pre-commit install            # installs the git hook; runs on every `git commit`
```

The hooks then run on staged files at commit time. To run them across the whole
repo on demand (useful the first time, or in CI):

```console
pre-commit run --all-files
```

> [!NOTE]
> The Airflow `AIR3` findings are **advisory, not enforced**. The Ruff hook runs
> with `--exit-zero` (and the CI lint job uses `continue-on-error`), so it
> *reports* Airflow 3.x deprecation warnings but never blocks your commit or the
> build. They flag Airflow-3 migration items — e.g. `airflow.decorators` /
> `airflow.models.Param` moving to `airflow.sdk` — that can't be fixed while the
> repo runs Airflow 2.10, so treat them as a heads-up for the eventual upgrade
> rather than something to resolve now.

See [Airflow's documentation on linting for best practices and how to contribute your own](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#code-quality-and-linting)

## Deployment Infrastructure

We have both dev and prod Airflow servers, although the dev server is not always running and there may not be feature parity between dev and prod (e.g., not all prod secrets have analogues in dev):

* `airflow-dev`: Hosted in the `dnt-dev` AWS account.
* `airflow-prod`: Hosted in the `dpe-prod` AWS account. Deployed using OpenTofu. Only accessible via [port forwarding](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod).
	* Deployed from the `main` branch in this repository.

Please see [Connecting to AWS EKS](https://sagebionetworks.jira.com/wiki/spaces/DPE/pages/3389325317/Connecting+to+AWS+EKS+Kubernetes+K8s+cluster) on Confluence if you want to interface with the EKS/Kubernetes cluster. Otherwise, for local development you will likely only be interested in using AWS Secrets Manager as a backend for Airflow Secrets.

There is a helper script in this repository for accessing this Airflow server.

## DAG Development Best Practices

Follow these best practices when developing DAGs to ensure reliability and maintainability. For comprehensive guidance, refer to the [Airflow Best Practices documentation](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html).

### Communication Between Tasks

* **Treat tasks as transactions** - Tasks should produce the same outcome on every re-run and never produce incomplete results
* **Use XCom for small messages** - For passing small data between tasks in distributed environments
* **Choose appropriate storage for data files** - Use temporary files for small data that can be quickly transferred, and S3 for larger datasets. Pass file paths via XCom rather than storing files locally on workers
* **Store authentication securely** - Use [Airflow Connections](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html) instead of hardcoding passwords or tokens in tasks

### Code Quality

* **Avoid top-level code** - Minimize code outside of operators and DAG definitions to improve scheduler performance and scalability
* **Use local imports** - Import heavy libraries inside task functions rather than at the top level to reduce DAG parsing time

### Shared Utilities

The repository provides shared helper functions in `src.utils` for common DAG development tasks. Prefer using these helpers rather than duplicating the logic in individual DAGs.

#### Logging

Use the shared `get_logger()` helper instead of creating loggers directly. It integrates with Airflow's logging configuration and prefixes logger names with `sage_airflow` to avoid conflicts with other loggers.

```python
from src.utils import get_logger

logger = get_logger(__name__)

logger.info("Starting monthly metrics export")
logger.warning("Skipping optional step")
logger.exception("Failed to upload report")
```

Passing `__name__` is recommended so log messages are associated with the module that emitted them.

#### Validating Required Secrets

DAGs that depend on Airflow Connections or Variables should validate that those secrets exist before attempting to use them. This allows a DAG to fail fast with a clear error message instead of failing later with a less informative exception.

Use `validate_required_secrets()` near the beginning of the DAG (or before secrets are first used):

```python
from src.utils import validate_required_secrets

validate_required_secrets(
    connection_ids=[
        "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN",
        "SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN",
    ],
    variable_names=[
        "ZENODO_API_TOKEN",
    ],
)
```

If any required Connection or Variable cannot be resolved, the helper raises a `ValueError` listing every missing secret, for example:

```text
Missing required secrets before running locally:
  connection: SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN
  variable: ZENODO_API_TOKEN
```

This is particularly useful when developing locally with `LocalFilesystemBackend`, where missing connections or variables are a common source of configuration errors.

### Testing and Validation

* **Test DAGs in Codespaces first** - Always test new DAGs thoroughly in your GitHub Codespaces development environment before deploying to production
* **Verify task functionality** - Ensure all tasks execute successfully and produce expected outputs in the development environment
* **Validate production deployment** - After deploying to production, monitor initial DAG runs to confirm they function as expected with production data and resources

### DAG Failure Alerts

Production DAGs should surface failures rather than relying on
someone watching the Airflow UI. There are two pieces to this:

**Send alerts via Synapse messages (delivered as email).** Since our DAGs
already use a `SynapseHook`, the simplest, dependency-free way to notify people
is [`synapse_client.sendMessage`](https://python-docs.synapse.org/en/stable/reference/client/?h=sendmessage#synapseclient.Synapse.sendMessage), which Synapse delivers as an
email. Rather than calling it directly, use the shared helper
`send_synapse_message(conn_id, usernames, subject, body)` in
[dags/src/synapse_alerts.py](./dags/src/synapse_alerts.py) — it resolves Synapse
usernames/numeric ids to owner ids and sends the message for you. This avoids
configuring SMTP on the Airflow deployment and works for **any** alert (task
failures, "report is ready" notices, data-quality warnings, etc.).

**Link failure alerts to the DAG's `on_failure_callback`.** For the common case,
email a failure alert to the DAG's own dev list, use the shared
`synapse_failure_callback(...)` function (also in
[dags/src/synapse_alerts.py](./dags/src/synapse_alerts.py)). It returns a
DAG-level `on_failure_callback` that, when a task fails (after retries are
exhausted) and the DagRun ends as failed, reads the Synapse connection id and
recipient list from your DAG params, builds a standard failure message (DAG id,
task id, run id, execution date, exception, and a link to the task logs), and
appends your optional DAG-specific note. The send is wrapped in a `try/except` so
a notification failure can never mask the original task error.
[See Airflow's callback docs for more info.](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html#callbacks)

```python
from airflow.decorators import dag
from airflow.models import Param

from src.synapse_alerts import synapse_failure_callback


@dag(
    on_failure_callback=synapse_failure_callback(
        message="This may indicate a Zenodo API schema change.",  # optional note
    ),
    params={
        "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
        # who to alert on failure (comma-separated usernames or numeric ids)
        "dev_user_list": Param("3460442", type="string"),
    },
    ...
)
def my_dag():
    ...
```

By default the callback reads the `synapse_conn_id` and `dev_user_list` params;
pass `conn_id_param` / `user_list_param` to `synapse_failure_callback(...)` if
your DAG names them differently. See it wired up in
[dags/zenodo_tep_metrics_dag.py](./dags/zenodo_tep_metrics_dag.py).

**Testing the alert:**

- **A DAG level `on_failure_callback` is run by the scheduler**, *not* by
  `dag.test()` / `airflow dags test`. It fires in **any environment with a
  running scheduler**, including the local docker-compose Airflow (webserver +
  scheduler + workers), so this is *not* production-only. To exercise it
  end-to-end, bring the stack up and trigger a run with a task forced to fail
  (e.g. temporarily raise inside a task):

  ```console
  docker compose up --build --detach
  ./airflow.sh dags trigger <dag_id>          # unpause first if needed
  docker compose logs -f airflow-scheduler    # watch the callback fire
  ```

  Note the callback firing is separate from the email arriving: `sendMessage`
  only emails if the `SynapseHook` connection/token resolves (configured in
  Codespaces; a pure-local docker run needs AWS creds or `SYNAPSE_AUTH_TOKEN`).
- For a quick local check via `python dags/<dag>.py` (which calls `dag.test()`),
  attach the same callback at the **task** level
  (`@task(on_failure_callback=synapse_failure_callback(...))`), since task-level
  callbacks *do* run under `dag.test()`.
- Set `retries` to `0` while testing so the failure (and the callback) fire
  immediately instead of after the retry delay (Airflow's default is ~5 min per
  retry).
- The Synapse send email only succeeds if the connection/token resolves; otherwise the
  guarded `try/except` logs "Failed to send failure alert", which still confirms
  the callback ran.

## Secrets

Airflow secrets (_e.g._ connections and variables) are stored in Secrets Manager within the `dpe-prod` AWS account. This includes some of the following connection IDs that are already in-place ready to use:

1. `synapse_conn_id`: `"SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN"`
   - The Airflow connection ID used to connect to the Synapse service. Use  to run Synapse commands. This connection was created under the Synapse orca service account.
1. `aws_conn_id`: `"AWS_TOWER_PROD_S3_CONN"`
   - The Airflow connection ID used to connect to AWS service.
1. `snowflake_conn_id`: `"SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN"`
   - The Airflow connection ID used to connect to the Snowflake service. Use this to run Snowflake commands as `DEVELOPER_SERVICE` in the `DATA_ENGINEER` role.

These are all located in AWS Secrets Manager in the `dpe-prod` account.

Developers access these secrets with their own AWS Identity Center (SSO) credentials — there is no shared IAM user or long-lived access key to manage or rotate. Set up an SSO profile once and log in per the [AWS credentials](./README.md#aws-credentials-required-for-all-options) section of the README (the source of truth for the `aws configure sso` / `aws sso login` steps). In deployed Airflow, the pods assume an IRSA role (`airflow-croissant`) and no credentials are stored in the connection secrets.

### Creating a new secret

New secrets must be created in AWS Secrets Manager in the `dpe-prod` account.

1. You will need at least **Developer** (or **Administrator**) access to the account.
1. Make sure your region is set to **us-east-1**.
1. For connection URIs, the secret name should have the prefix `airflow/connections/`
(i.e. `airflow/connections/MY_SECRET_CONNECTION_STRING`). Variables should have the prefix `airflow/variables/` (i.e. `airflow/variables/MY_SECRET_VARIABLE`).
1. Secret type to use is **Other type of secret**

### Configuring a SynapseHook connection secret

See [Synapse credentials section in the py-orca's env.example](https://github.com/Sage-Bionetworks-Workflows/py-orca/blob/main/.env.example) for the expected format of your custom `SYNAPSE_CONNECTION_URI`.

### Configuring a SnowflakeHook connection secret

Typically, Snowflake secrets are used in accordance with Snowflake Service Users. Please refer to the [Snowflake Service Accounts documentation](https://sagebionetworks.jira.com/wiki/spaces/DPE/pages/3431628815/Snowflake+Service+Accounts) first if this is the case for you.

1. There are two ways to [configure a snowflake secret based on the documentation](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html#configuring-the-connection) either through key-value pairs or serializing with json. Based on the snowflake documentation, all of the following parameters are marked as "optional", but you still have to specify the parameters and their values:
   - warehouse
   - role
   - account
   - private_key_content
   - authenticator

If choosing the [serializing with json method](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html#json-format-example), this is a way to turn the private key to a single line that could fit into the json under `private_key_content`:

```python
def single_liner():
    with open("<path_to_private_key_file>", "r") as f:
        lines = f.read().splitlines()

    single_line = "\n".join(lines)
    print(single_line)

single_liner()
```

Within a DAG, you can then use your connection when instantiating a `Hook` [object](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html#hooks), like:

Example for Synapse connections:

```python
from orca.services.synapse import SynapseHook

syn_hook = SynapseHook("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN")
```

Example for Snowflake connections:

```python
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

snow_hook = SnowflakeHook("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN")
```

or your secret variable, like:

```python
my_secret_variable = Variable.get("MY_SECRET_VARIABLE_CONNECTION_STRING")
```

During DAG development and testing, you can create a secret containing the connection URI (or secret variable) for development resources (such as Nextflow Tower Dev). Once you are ready to run the DAG in production, you can update the secret value with a connection URI for production resources (such as Nextflow Tower Prod).

## Contributing a Challenge DAG

As part of propping up a new Synapse-hosted challenge, the workflow you choose in [nf-synapse-challenge](https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge/tree/main) will need to
be automated to run for challenge submissions on a rolling basis. For this we leverage Airflow DAGs to perform these
automated executions of your customized challenge workflow.

This section describes how you can contribute a new challenge DAG by configuring the `challenge_configs.yaml` file
and the DAG factory pattern in this repository.

----

### TL;DR

To add a DAG for your challenge, make a pull request introducing a profile to the `challenge_configs.yaml` file with the parameters listed in [Creating Your Challenge DAG Config](#creating-your-challenge-dag-config).
See below for a template you can follow.

Modify everything in `<>` and remove the `<>` when complete:

```
<my-challenge>:
  synapse_conn_id: "<MY_SYNAPSE_CONN_ID>"
  aws_conn_id: "<MY_AWS_CONN_ID>"
  revision: "<1a2b3c4d>"
  challenge_profile: "<my_challenge_profile>"
  tower_conn_id: "<MY_CHALLENGE_PROJECT_TOWER_CONN>"
  tower_view_id: "<syn123>"
  tower_compute_env_type: "spot"
  bucket_name: "<my-challenge-project-tower-scratch>"
  key: "<10days/my_challenge>"
  dag_config:
    schedule_interval: "*/1 * * * *"
    start_date: "<YYYY>-<MM>-<DD>T<HH>:<MM>:<SS>+00:00" # Will be converted to a UTC datetime object
    end_date: "<YYYY>-<MM>-<DD>T<HH>:<MM>:<SS>+00:00" # Will be converted to a UTC datetime object
    catchup: false
    default_args:
      retries: 2
    tags:
      - "nextflow_tower"
```

> [!NOTE]
> While `synapse_conn_id` and `aws_conn_id` are customizable, we do have connection IDs already in-place for you to use that will connect you to Synapse (through DPE's ORCA Service Account) and to AWS. Feel free to swap these for your own if they do not suit your needs.

```
synapse_conn_id: "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN"
aws_conn_id: "AWS_TOWER_PROD_S3_CONN"
```

----

### Overview

For introducing a challenge DAG, there are two files that come into play:

1. The `challenge_configs.yaml` file, which holds configuration settings for each challenge DAG. This file allows you to easily add
   new challenges or adjust existing ones without modifying any Python DAG scripts directly.
   The configurations in this file are used by the DAG factory to dynamically create DAGs.

2. The `challenge_dag_factory.py` is a Python script that:
   * Loads challenge configurations from the YAML file.
   * Resolves the `dag_config`, that is, the configuration settings needed to generate the DAG dynamically at runtime.
   * Generates runtime parameters (such as a unique run-specific UUID) and injects them into the DAG.
   * Creates and wires tasks together to fetch submissions, update statuses, generate CSV manifests, launch workflows, and monitor execution.

By updating `challenge_configs.yaml` with your **challenge DAG config**, you can contribute new challenges without needing to change the underlying DAG generation code.

----

### Local development

By default, `challenge_dag_factory.py` loads `challenge_configs.yaml` from the production (`main`) branch of this repository. To test changes to `challenge_configs.yaml` before they are merged, set the `CONFIG_URL` environment variable to either a local copy of the YAML file or the raw GitHub URL for your feature branch.

For example, to test a feature branch:

```console
export CONFIG_URL="https://raw.githubusercontent.com/Sage-Bionetworks-Workflows/orca-recipes/<YOUR_BRANCH>/dags/challenge_configs.yaml"
```

### Creating Your Challenge DAG Config

Each challenge DAG config will have its own set of parameters depending on the customized workflow profile on [nf-synapse-challenge](https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge/blob/main/nextflow.config) and what part of Synapse and AWS S3 the workflow will
be communicating with.

See below for a list of parameters and their descriptions:

#### Parameters

1. `synapse_conn_id`: The Airflow connection ID used to connect to the Synapse service. This connection is utilized when fetching submission data and updating statuses. **Use `SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN`**.
1. `aws_conn_id`: The connection ID for AWS. This enables the DAG to upload CSV files (manifests) to an S3 bucket via the S3 hook. **Use `AWS_TOWER_PROD_S3_CONN`**.
1. `revision`: Specifies the version or Git commit revision of the `main` branch in the `nf-synapse-challenge` repository (which houses your workflow). This ensures that the correct version of your workflow is deployed when the DAG triggers a run. **Use `main` or a specific commit SHA which points to the desired version**.
1. `challenge_profile`: Identifies the Nextflow Tower challenge profile you contributed in `nextflow.config` of the `nf-synapse-challenge` repository. This parameter customizes the execution environment for the workflow. See example of a previous profile contribution [here](https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge/pull/47).
1. `tower_conn_id`: The Airflow connection ID to connection URI of the Seqera tower workspace for your challenge. This is needed so your challenge DAG can execute the workflow runs in your desired Seqera workspace.
1. `tower_view_id`: The identifier used to query the submission view on Synapse. It tells the DAG, to _tell the workflow_, where to look for submissions to fetch and process.
1. `tower_compute_env_type`: Indicates the compute environment (for example, `"spot"`) to be used when launching the workflow. **Use `spot` for challenges that will take less computational time to evaluate the submissions. Use `on-demand` otherwise**.
1. `bucket_name`: The S3 bucket where the challenge-related files (such as CSV manifests) will be stored. Note that the Seqera Platform workspaces can only access the S3 buckets assigned to them.
1. `key`: The S3 key prefix (or folder path) under which the submissions manifest file is uploaded for a challenge DAG run. At runtime, a unique run-specific UUID is appended to this key to ensure that files are uniquely identified and organized. Since this folder path lives in a scratch bucket, you can leverage one of the folders that are configured to delete stale objects based on a certain number of days ([see here](https://sagebionetworks.jira.com/wiki/spaces/WF/pages/2191556616/Getting+Started+with+Nextflow+and+Seqera+Platform#Tower-Project-Breakdown) for more details). This will affect what value you put here. For example, **if you would like for your manifest file to live for 10 days, use `10days/my_project_folder`**.
1. `dag_config`: A nested dictionary containing additional DAG scheduling and runtime parameters:
   * `schedule_interval`: A cron expression that determines how frequently the DAG is triggered.
   * `start_date`: An ISO 8601–formatted date-time string (e.g. `2025-07-16T08:00:00+02:00`) that tells the DAG factory when to begin scheduling runs. The trailing ±HH:MM UTC-offset makes the datetime timezone-aware, and the factory converts it into a Python datetime object in UTC.
   * `end_date`: An ISO 8601–formatted date-time string (e.g. `2025-07-20T00:00:00-05:00`) that indicates when the DAG should stop scheduling new runs. Like start_date, it may include a ±HH:MM UTC-offset and is parsed by the factory into a timezone-aware Python datetime object in UTC.
   * `catchup`: A Boolean flag that indicates whether Airflow should run missed DAG runs (catch up) if the scheduler falls behind.
   * `default_args`: Standard Airflow arguments for the DAG. For example, you can set the number of retries for tasks.
   * `tags`: A list of tags for categorizing the DAG in the Airflow UI.


### Contributing a New Challenge DAG

To contribute a new challenge:

1. **Add/Update Configuration**: Create a new section in `challenge_configs.yaml` with a unique challenge key. Provide all required parameters as described above.
1. **Validate the Parameters**: Make sure all connection IDs, S3 bucket names, and the revision match your environment and the workflow you intend to run.
1. **Submit a Pull Request**: Make a pull request to submit your changes. A repository maintainer will do a revision and work with you in the next step.
1. **Test in Your Environment**: Validate the new configuration by running your DAG in a codespace environment before merging and deploying to production.
1. **Merge your Pull Request**: Once your changes have been tested, your PR is ready for merge and a maintainer will ensure your DAG is created and running in Airflow production!

By following these guidelines, you will successfully contribute a deployable challenge DAG customized for your needs.

----

### Troubleshooting

This section is a living reference for potential issues you may encounter when testing your DAG. If you run into a problem not covered here, consider documenting it for future contributors.

#### Airflow Provider Hooks Failing to Connect — Expired SSO Credentials

If your Airflow provider hooks (e.g., `SynapseHook`, `SnowflakeHook`, `S3Hook`) are failing to connect, it is most likely because your AWS Identity Center (SSO) session has expired. Airflow resolves connections and variables from AWS Secrets Manager in `dpe-prod` using your own SSO credentials (see the [Secrets](#secrets) section). When the session expires, Airflow can no longer resolve any Secrets-Manager-backed connection or variable, which can surface as `KeyError` import errors, hook connection failures, or `botocore` token errors such as:
```
botocore.exceptions.ClientError: An error occurred (UnrecognizedClientException) when calling the GetSecretValue operation: The security token included in the request is invalid.
```
— even when the connection ID strings themselves look correct.

**To fix it, refresh your SSO session** by re-running the credential steps for your path in the README's [AWS credentials](./README.md#aws-credentials-required-for-all-options) section (`aws sso login`, plus `scripts/aws-sso-to-env.sh` + a stack restart in Codespaces).

If it still fails, confirm your profile is configured and you are assigned the `Developer` (or `Administrator`) role in `dpe-prod` (`766808016710`):
```console
aws sts get-caller-identity --profile <your-sso-profile>
```

# Contribution Guidelines

## Development

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

### Testing and Validation
* **Test DAGs in Codespaces first** - Always test new DAGs thoroughly in your GitHub Codespaces development environment before deploying to production
* **Verify task functionality** - Ensure all tasks execute successfully and produce expected outputs in the development environment
* **Validate production deployment** - After deploying to production, monitor initial DAG runs to confirm they function as expected with production data and resources

## Secrets

Airflow secrets (_e.g._ connections and variables) are stored in Secrets Manager within the `dpe-prod` AWS account. This repository uses an IAM User `airflow-secrets-backend` to access the secrets. Access keys for the IAM account are stored in this repository as codespace secrets, enabling Airflow deployments in our configured codespaces environment to retrieve connection URIs and secret variables from `dpe-prod`. The credentials used in the repository's codespace secrets must be rotated manually within the AWS console, and updated every 90 days.

### Creating a new secret

New secrets must be created in AWS Secrets Manager in the `dpe-prod` account.

1. You will need at least **Developer** access to the account.
1. Make sure your region is set to **us-east-1**.
1. For connection URIs, the secret name should have the prefix `airflow/connections/`
(i.e. `airflow/connections/MY_SECRET_CONNECTION_STRING`). Variables should have the prefix `airflow/variables/` (i.e. `airflow/variables/MY_SECRET_VARIABLE`).
1. Secret type is **Other type of secret**
1. There are two ways to [configure the secrets based on the documentation](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html#configuring-the-connection) either through key-value pairs or serializing with json. Based on the snowflake documentation, all these parameters are "optional", but you still have to specify the following parameters and their values:
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

```python
from orca.services.synapse import SynapseHook

syn_hook = SynapseHook("MY_SECRET_CONNECTION_STRING")
```

or your secret variable, like:

```python
my_secret_variable = Variable.get("MY_SECRET_VARIABLE")
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

Note that while `synapse_conn_id` and `aws_conn_id` are customizable, we do have connection IDs already in-place
for you to use that will connect you to Synapse (through DPE's ORCA Service Account) and to AWS. But feel free to swap
these for your own if they do not suit your needs.

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

# ORCA Recipes

This repository contains recipes (DAGs) for data processing and engineering at Sage Bionetworks.

## Airflow

### Quick Start

This assumes that you have Docker installed with [Docker Compose V2](https://docs.docker.com/compose/compose-v2/). It's recommended that you leverage the included Dev Container definition (_i.e._ `devcontainer.json`) to standardize your development environment. You can use the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) VS Code extension or GitHub Codespaces.

```console
# Duplicate example `.env` file and edit as needed
cp .env.example .env
docker compose up --build --detach
```

If you encounter the `nginx bad gateway` errors when navigating to the forwarded port, just wait and refresh a couple of times. Airflow takes a few minutes to become available.

Any edits to your DAG should get picked up by Airflow automatically. If you're not seeing that happen, you can try restarting the containers as follows.

```console
docker compose restart
```

If you edit `Dockerfile`, `docker-compose.yaml`, `Pipfile`/`Pipfile.lock`, `airflow.cfg`, or `.env`, you'll need to rebuild the containers as follows.

```console
# For example, you can update the dependencies in Pipfile.lock using:
# pipenv lock --dev
docker compose down
docker compose up --build --detach
```

If you want to run commands in the "Airflow context" (_i.e._ within the custom containers), you can use the included `airflow.sh as follows.

```console
# Start a shell inside one of the containers
./airflow.sh bash

# Start a Python REPL inside one of the containers
./airflow.sh python

# Run an Airflow CLI command
./airflow.sh info
```

### Logging in

When deploying airflow locally on dev containers, the username and password will be "airflow".

## Local DAGs

### Usage

This repository also contains recipes for specific projects that either don't need to be deployed to Airflow or are not ready to be deployed to Airflow. These recipes can be run locally from the `local` directory. Each sub-directory contains recipes specific to a project and those project folders have their own documentation for running the recipes.

### Dependencies

Dependencies for `local` recipes are defined in the requirements files at the root of this repository. To get started, you can install all of the dependencies that you might need by running:
```console
bash dev_setup.sh
source venv/bin/activate
```
This will create a virtual environment with Python version 3.10 and all needed dependencies and activate it. Before running, be sure to have Python 3.10 or `pyenv` installed on your machine.

## Building a new docker image

At the moment this is a manual process to build and push a new `orca-recipes` container
to GHCR. In order to accomplish this task:

1. Create a new github codespaces environment detailed above.
2. Build the new image with the specific version you want: `docker build -t ghcr.io/sage-bionetworks-workflows/orca-recipes:0.0.0 .`
3. Ensure you are logged into the GHCR: `echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin`
   1. Replace `$GITHUB_TOKEN` and `USERNAME` with the correct values
4. Push the image to GHCR for your version: `docker push ghcr.io/sage-bionetworks-workflows/orca-recipes:0.0.0`

## Contributing a Challenge DAG 

This section describes how you can contribute a new challenge DAG by configuring the `challenge_configs.yaml` file
and the DAG factory pattern in this repository.

### TL;DR

To add a challenge DAG, make a pull request introducing a profile to the `challenge_configs.yaml` file with the available parameters (LINK).
See below for a template you can follow:

```
test:
  synapse_conn_id: "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN"
  aws_conn_id: "AWS_TOWER_PROD_S3_CONN"
  revision: "1a2b3c4d" # Updated to the latest revision with new params.entry for PEGS
  challenge_profile: "test_challenge_profile"
  tower_conn_id: "TEST_CHALLENGE_PROJECT_TOWER_CONN"
  tower_view_id: "syn123"
  tower_compute_env_type: "spot"
  bucket_name: "test-challenge-project-tower-scratch"
  key: "10days/test_challenge"
```

### Overview
For introducing a challenge DAG, there are two files that come into play:

1. The `challenge_configs.yaml` file, which holds configuration settings for each challenge DAG. This file allows you to easily add new challenges or adjust existing ones without modifying any Python DAG scripts directly. The configurations in this file are used by the DAG factory to dynamically create DAGs.

The DAG factory is a Python function that:

    Loads challenge configurations from the YAML file.

    Merges default DAG settings (e.g., schedule, start date, retries) with custom settings.

    Generates runtime parameters (such as a unique run-specific UUID) and injects them into the DAG.

    Creates and wires tasks together to fetch submissions, update statuses, generate CSV manifests, launch workflows, and monitor execution.

By updating challenge_configs.yaml, you can contribute new challenges without needing to change the underlying DAG generation code.
challenge_configs.yaml Explained

Each challenge has its own set of parameters. Below is an example configuration with two challenges (named pegs and test):

pegs:
  synapse_conn_id: "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN"
  aws_conn_id: "AWS_TOWER_PROD_S3_CONN"
  revision: "558a3786e1610a0cb802e384ab082f696030809a" # Updated to the latest revision with new params.entry for PEGS
  challenge_profile: "pegs_challenge_test"
  tower_conn_id: "PEGS_CHALLENGE_PROJECT_TOWER_CONN"
  tower_view_id: "syn58942525"
  tower_compute_env_type: "spot"
  bucket_name: "pegs-challenge-project-tower-scratch"
  key: "10days/pegs_challenge"
  dag_config:
    schedule_interval: "*/1 * * * *"
    start_date: "2024-04-09T00:00:00"  # ISO-format date string; will be converted in the DAG factory
    catchup: false
    default_args:
      retries: 2
    tags:
      - "nextflow_tower"

test:
  synapse_conn_id: "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN"
  aws_conn_id: "AWS_TOWER_PROD_S3_CONN"
  revision: "1a2b3c4d" # Updated to the latest revision with new params.entry for PEGS
  challenge_profile: "test_challenge_profile"
  tower_conn_id: "TEST_CHALLENGE_PROJECT_TOWER_CONN"
  tower_view_id: "syn123"
  tower_compute_env_type: "spot"
  bucket_name: "test-challenge-project-tower-scratch"
  key: "10days/test_challenge"

Parameter Descriptions

    synapse_conn_id
    The Airflow connection ID used to connect to the Synapse service. This connection is utilized when fetching submission data and updating statuses.

    aws_conn_id
    The connection ID for AWS. This enables the DAG to upload CSV files (manifests) to an S3 bucket via the S3 hook.

    revision
    Specifies the version or Git commit revision of the workflow. This ensures that the correct version of your pipeline is deployed when the DAG triggers a workflow.

    challenge_profile
    Identifies the Nextflow Tower challenge profile. This parameter customizes the execution environment for the workflow.

    tower_conn_id
    The Airflow connection ID for Nextflow Tower, used when launching and monitoring the workflow execution.

    tower_view_id
    The identifier used to query the Nextflow Tower view. It tells the DAG which submissions to fetch and process.

    tower_compute_env_type
    Indicates the compute environment (for example, "spot") to be used when launching the workflow. This typically relates to cost or performance decisions.

    bucket_name
    The S3 bucket where the challenge-related files (such as CSV manifests) will be stored.

    key
    The S3 key prefix (or folder path) under which files are uploaded. At runtime, a unique run-specific UUID is appended to this key to ensure that files are uniquely identified and organized.

    dag_config
    A nested dictionary containing additional DAG scheduling and runtime parameters:

        schedule_interval:
        A cron expression that determines how frequently the DAG is triggered.

        start_date:
        An ISO-formatted date string that specifies when the DAG should start running. The DAG factory converts this to a Python datetime object.

        catchup:
        A Boolean flag that indicates whether Airflow should run missed DAG runs (catch up) if the scheduler falls behind.

        default_args:
        Standard Airflow arguments for the DAG. For example, you can set the number of retries for tasks.

        tags:
        A list of tags for categorizing the DAG in the Airflow UI.

How It All Works Together

    Configuration Loading:
    The DAG factory reads challenge_configs.yaml to load the configuration for each challenge.

    DAG Creation:
    For each challenge configuration, the factory creates a new DAG with the provided connection IDs, file upload settings, and scheduling parameters. Dynamic values, such as a unique run UUID, are generated at runtime.

    Task Dependencies:
    The created DAG consists of several tasks (submission fetching, status updating, CSV generation, workflow launching, and monitoring) that depend on each other. The unique run UUID is used to tag outputs (like the S3 key) to ensure that each run is isolated.

Contributing a New Challenge DAG

To contribute a new challenge:

    Add/Update Configuration:
    Create a new section in challenge_configs.yaml with a unique challenge key. Provide all required parameters as described above.

    Validate the Parameters:
    Make sure all connection IDs, S3 bucket names, and the revision match your environment and the workflow you intend to run.

    Test in Your Environment:
    Validate the new configuration by running your DAG in a local or staging environment before deploying to production.

    Submit a Pull Request:
    Follow the project's contribution guidelines to submit your changes.

By following these guidelines, you help maintain a consistent and scalable way to add new challenges without modifying the core DAG creation logic.

This documentation should help contributors understand the purpose of each parameter in the YAML file and how they affect the creation and operation of the DAGs.
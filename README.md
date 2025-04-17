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

----

### TL;DR

To add a DAG for your challenge, make a pull request introducing a profile to the `challenge_configs.yaml` file with the parameters listed in (TODO LINK).
See below for a template you can follow. Modify everything in `<>` and remove the `<>` when complete:

```
<my-challenge>:
  synapse_conn_id: "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN"
  aws_conn_id: "AWS_TOWER_PROD_S3_CONN"
  revision: "<1a2b3c4d>"
  challenge_profile: "<my_challenge_profile>"
  tower_conn_id: "<MY_CHALLENGE_PROJECT_TOWER_CONN>"
  tower_view_id: "<syn123>"
  tower_compute_env_type: "spot"
  bucket_name: "<my-challenge-project-tower-scratch>"
  key: "<10days/my_challenge>"
  dag_config:
    schedule_interval: "*/1 * * * *"
    start_date: "2024-04-09T00:00:00"
    catchup: false
    default_args:
      retries: 2
    tags:
      - "nextflow_tower"
```

----

### Overview

As part of propping up a new Synapse-hosted challenge, the workflow you choose in [nf-synapse-challenge](https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge/tree/main) will need to
be automated to run for challenge submissions on a rolling basis. For this we leverage Airflow DAGs to perform these
automated executions of your customized challenge workflow.

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

1. synapse_conn_id: The Airflow connection ID used to connect to the Synapse service. This connection is utilized when fetching submission data and updating statuses. **Use `SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN`**.
1. aws_conn_id: The connection ID for AWS. This enables the DAG to upload CSV files (manifests) to an S3 bucket via the S3 hook. **Use `AWS_TOWER_PROD_S3_CONN`**.
1. revision: Specifies the version or Git commit revision of the `main` branch in the `nf-synapse-challenge` repository (which houses your workflow). This ensures that the correct version of your workflow is deployed when the DAG triggers a run. **Use `main` or a specific commit SHA which points to the desired version**.
1. challenge_profile: Identifies the Nextflow Tower challenge profile you contributed in `nextflow.config` of the `nf-synapse-challenge` repository. This parameter customizes the execution environment for the workflow.
1. tower_conn_id: The Airflow connection ID for the Nextflow Tower (Seqera) workspace used when launching and monitoring the workflow execution.
1. tower_view_id: The identifier used to query the submission view on Synapse. It tells the DAG, to _tell the workflow_, where to look for submissions to fetch and process.
1. tower_compute_env_type: Indicates the compute environment (for example, "spot") to be used when launching the workflow. **Use `spot` for challenges that will take less computational time to evaluate the submissions. Use `on-demand` otherwise**.
1. bucket_name: The S3 bucket where the challenge-related files (such as CSV manifests) will be stored.
1. key: The S3 key prefix (or folder path) under which files are uploaded. At runtime, a unique run-specific UUID is appended to this key to ensure that files are uniquely identified and organized. Since this folder
   path lives in a scratch bucket, you can leverage one of the folders that are configured to delete stale objects based on a certain number of days ([see here](https://sagebionetworks.jira.com/wiki/spaces/WF/pages/2191556616/Getting+Started+with+Nextflow+and+Seqera+Platform#Tower-Project-Breakdown) for more details). This will affect what value you put here. For example, **if you would like for these workflow outputs to live for 10 days, use `10days/my_project_folder`**.
1. dag_config: A nested dictionary containing additional DAG scheduling and runtime parameters:
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

By following these guidelines, you will successfully contribute a deployable challenge DAG customized for your needs.

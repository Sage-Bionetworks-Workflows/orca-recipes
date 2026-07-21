# ORCA Recipes

## Table of Contents

- [Example Workflows](#example-workflows)
- [Airflow Development](#airflow-development)
  - [Setting up the Dev Environment](#setting-up-the-dev-environment)
    - [AWS credentials](#aws-credentials-required-for-all-options)
    - [Dev Container](#dev-container)
      - [Codespaces](#codespaces)
      - [VS Code](#vs-code)
    - [Docker Compose](#docker-compose)
  - [Interfacing with Airflow](#interfacing-with-airflow)
    - [CLI](#cli)
    - [Browser](#browser)
  - [Local Development (without Dev Container)](#local-development-without-dev-container)
    - [1. Install dependencies](#1-install-dependencies)
    - [2. Initialize the Airflow database](#2-initialize-the-airflow-database)
    - [3. Configure environment variables](#3-configure-environment-variables)
    - [4. Run a DAG locally](#4-run-a-dag-locally)
- [Local DAGs](#local-dags)
- [Contributing](#contributing)
- [Releases](#releases)

This repository contains Airflow recipes (DAGs) for data processing and engineering at Sage Bionetworks. If you want to develop a workflow to process data, you've come to the right place.

## Example Workflows

- **Challenge Automation** - Automatically evaluate challenge submissions by fetching entries from Synapse and orchestrating Nextflow workflows via Seqera Platform.
- **Dataset Discovery** - Generate Croissant-format metadata for Synapse datasets and publish to public S3 for improved discoverability.
- **Analytics Pipelines** - Sync Synapse Portal data to Snowflake and generate platform usage reports tracking downloads, users, and storage.
- **Bioinformatics QC** - Launch and monitor data quality control workflows for genomics projects (GENIE, HTAN).

## Airflow Development

For instructions on how to set up the development environment and interface with Airflow, see below. For a detailed guide on how to add or update DAGs and make changes to the Airflow infrastructure or environment, see [CONTRIBUTING.md](./CONTRIBUTING.md).

### Setting up the Dev Environment

A complete Airflow deployment is made up of multiple services running in parallel, so the steps involved in setting up a dev environment are more complex than you may be used to. There are two steps involved in setting up Airflow for development:

1. (Highly recommended) Develop within the provided dev container. A dev container is a virtual machine (e.g., Docker container) that standardizes tools, libraries, and configs for consistent development across machines. This provides us with a consistent environment for the next step.
2. Run docker compose to deploy the full suite of containerized services.

Regardless of which path you take — Codespaces, a local dev container, or [local development without a dev container](#local-development-without-dev-container) — you first need AWS credentials, since every path resolves Airflow's secrets from AWS Secrets Manager.

#### AWS credentials (required for all options)

The Airflow Secrets backend reads connections and variables from AWS Secrets Manager in the `dpe-prod` account. Authenticate with your **own** AWS Identity Center (SSO) credentials — there is no shared IAM user.

**One-time setup:** configure an SSO profile with `aws configure sso` using start URL `https://d-906769aa66.awsapps.com/start`, region `us-east-1`, account `766808016710`, and the `Developer` (or `Administrator`) role.

**Every session:** log in to refresh your SSO token (this is the common first step for all paths below):

```console
aws sso login --profile <your-sso-profile>
```

Then supply the credentials to Airflow, which differs by path:

* **Local Dev Container / VS Code** — set `AWS_PROFILE` (and, if your AWS config is not at `$HOME/.aws`, `HOST_AWS_DIR`) in `.env`. Your `~/.aws` is mounted read-only into the containers and SSO tokens refresh automatically.
* **Codespaces** — run `bash scripts/aws-sso-to-env.sh <your-sso-profile>` to write short-lived credentials into `.env` (there is no host `~/.aws` to mount). They expire; re-run the script to refresh.
* **Local without a dev container** — point the secrets backend at your profile via `AIRFLOW__SECRETS__BACKEND_KWARGS`; see [that section](#3-configure-environment-variables).

#### Dev Container

There are multiple ways to set up and interface with a dev container, depending on whether you want an IDE-agnostic approach, a VS Code workflow with the Dev Containers extension, or a cloud option like GitHub Codespaces. The cloud option is the most straightforward, although because the infrastructure is running in the cloud, there is a limit on how much time we can develop before we need to pay for the service.

* Note: The environment setup for the Dev Container is defined in [Dockerfile](./Dockerfile). _How_ we deploy the container locally is defined in [devcontainer.json](.devcontainer/devcontainer.json).

##### Codespaces

1. Create a branch for your changes
2. From the main repo page click on `<> Code`
3. Under `Codespaces` click the 3 ellipses and `New with options...`
4. Choose your branch and 4-core (2-core is sufficient for basic edits without docker compose).

##### VS Code

Visual Studio Code provides an extension so that your IDE terminal and other development tools are run within a dev container. Follow the instructions [here](https://code.visualstudio.com/docs/devcontainers/tutorial) to set up the Dev Containers extension. Do not create a new dev container, but rather use the existing configuration by opening the Command Palette (CMD+Shift+p by default on Mac) → "Dev Containers: Reopen in Container."

Set `AWS_PROFILE` in `.env` (see [AWS credentials](#aws-credentials-required-for-all-options) above) so the container can reach the Airflow Secrets backend. Alternatively, you can [connect to Codespaces as a remote environment from within VS Code](https://docs.github.com/en/codespaces/developing-in-a-codespace/using-github-codespaces-in-visual-studio-code).

#### Docker Compose

Ensure that your Docker installation is up to date (we use [Docker Compose V2](https://docs.docker.com/compose/compose-v2/)). It's recommended that you deploy from within the included dev container (previous section).

We pass environment variables to our build via the `.env` file. Configure your AWS credentials there following the [AWS credentials](#aws-credentials-required-for-all-options) section above (set `AWS_PROFILE` locally, or run `scripts/aws-sso-to-env.sh` in Codespaces).

```console
# Duplicate example `.env` file, then set your AWS credentials in it.
cp .env.example .env
```

To build and deploy our Airflow services to background containers:

```console
docker compose up --build --detach
```

Congrats! You have completed set up of the dev environment.

### Interfacing with Airflow

Airflow is made up of multiple components or services working together. The webserver exposes a browser-accessible port, but in a development environment we often want to interface with Airflow through its CLI.

#### CLI

We can see which services are currently running:
```console
# list running docker containers
docker compose ps

# see stats
docker compose stats
```

We provide a convenience script [airflow.sh](./airflow.sh) to invoke the Airflow CLI within the same container environment as the webserver/scheduler:

```console
# Start a shell inside one of the containers
./airflow.sh bash

# List our DAGs
./airflow.sh dags list
```

It may be helpful at this point to verify that Airflow has access to the secrets backend.
```console
./airflow.sh python
```
```python
import boto3

secretsmanager = boto3.client("secretsmanager", region_name='us-east-1')
secret_prefixes = ["airflow/connections/", "airflow/variables/"] # see secrets.backend_kwargs in airflow.cfg
all_secrets = secretsmanager.list_secrets()["SecretList"]
for secret in all_secrets:
    if any([secret["Name"].startswith(p) for p in secret_prefixes]):
        print(secret["Name"])
```

#### Browser

We can see which ports our Airflow services expose under `PORTS`:
```
docker compose ps
```

Airflow’s webserver listens on port 8080 by default via the localhost url: http://localhost:8080. You can access the local server in two ways:

- If you run the service locally on your machine, open the localhost url link in your browser.
- If you run the service inside a GitHub Codespace, localhost:8080 refers to the Codespace virtual machine, not your computer. In that case, use the forwarded port URL (e.g., `https://<codespace-name>-8080.app.github.dev`) or Ctrl/Cmd-click the localhost url link inside the Codespace to automatically use the correct forwarded URL

The username and password will be "airflow".

If you encounter the `nginx bad gateway` errors when navigating to the forwarded port, just wait and refresh a couple of times. Airflow takes a few minutes to become available.

### Local Development (without Dev Container)

If you want to test a DAG locally without Docker or Codespaces, you can run it directly against a local Airflow SQLite database.

#### 1. Install dependencies

There is a shell script that you can run to install the local Airflow requirements.

```console
bash scripts/install_local_airflow.sh
```

This separate shell script is needed because to avoid a `urllib3` version conflict introduced by the Airflow constraints file. There is also a known issue with `setuptools >82` as `synapseclient` (via `opentelemetry-instrumentation`) depends on the deprecated `pkg_resources` module, which `setuptools` 82+ removed. We pin that until `opentelemetry-instrumentation` drops the pkg_resources import.

#### 2. Initialize the Airflow database

```console
airflow db migrate
```

This creates the local SQLite metadata database (including the `task_instance` table) that Airflow needs to run `dag.test()`.

#### 3. Configure environment variables

Prior to configuring environment variables, configure and log in to an SSO profile with read access to the `dpe-prod` Secrets Manager secrets (`airflow/connections/`, `airflow/variables/`) — see [AWS credentials](#aws-credentials-required-for-all-options) for the one-time setup and `aws sso login` step. Unlike the container paths, here you point Airflow's secrets backend at the profile directly (below) rather than via `.env`.

Set the following exports (e.g., in your shell profile) so Airflow can resolve connections and variables from AWS Secrets Manager and deserialize custom dataclasses passed between tasks:

```console
export AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend

export AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables", "profile_name": "<your-aws-profile>"}'

export AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES="airflow.* astro.* __main__.*"
```

#### 4. Run a DAG locally

```console
python dags/<your-dag-file>.py
```

DAG files with an `if __name__ == "__main__":` block that calls `dag.test()` can be run directly this way.

See section [Testing](./CONTRIBUTING.md#testing) in the CONTRIBUTING.md for more info on testing a dag and how you can bypass configuring with AWS.


### Local DAGs

This repository also contains recipes for specific projects that either don't need to be deployed to Airflow or are not ready to be deployed to Airflow. These recipes can be run locally from the `local` directory. Each sub-directory contains recipes specific to a project and those project folders have their own documentation for running the recipes.

For local development outside of Docker, we provide a convenience script to set up a Python virtual environment:

```console
bash local/dev_setup.sh
source venv/bin/activate
```

## Contributing

For detailed contribution guidelines, repository structure, and testing instructions, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Releases

To release a new version of the `orca-recipes` container to GHCR:

1. Create a new GitHub Release in the repository
   - Go to the repository's "Releases" page
   - Click "Create a new release"
   - Create a new tag with the version number (e.g., `1.0.0`)
   - Add release notes
   - Click "Publish release"

The GitHub Actions workflow will automatically:
- Build the Docker image
- Tag it with the release version
- Push it to GHCR

The `latest` tag will automatically be updated to point to the latest release.

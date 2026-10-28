# ORCA Recipes

## Table of Contents

- [Example Workflows](#example-workflows)
- [Airflow Development](#airflow-development)
  - [Setting up the Dev Environment](#setting-up-the-dev-environment)
    - [AWS credentials](#aws-credentials)
  - [Local Development](#local-development)
    - [1. Install dependencies](#1-install-dependencies)
    - [2. Initialize the Airflow database](#2-initialize-the-airflow-database)
    - [3. Configure environment variables](#3-configure-environment-variables)
    - [4. Run a DAG locally](#4-run-a-dag-locally)
    - [5. Run the scheduler and UI](#5-run-the-scheduler-and-ui)
- [Local DAGs](#local-dags)
- [Contributing](#contributing)

This repository contains Airflow recipes (DAGs) for data processing and engineering at Sage Bionetworks. If you want to develop a workflow to process data, you've come to the right place.

## Example Workflows

- **Dataset Discovery** - Generate Croissant-format metadata for Synapse datasets and publish to public S3 for improved discoverability.
- **Analytics Pipelines** - Sync Synapse Portal data to Snowflake and generate platform usage reports tracking downloads, users, and storage.
- **Bioinformatics QC** - Launch and monitor data quality control workflows for genomics projects (GENIE, HTAN).

## Airflow Development

For instructions on how to set up the development environment and interface with Airflow, see below. For a detailed guide on how to add or update DAGs and make changes to the Airflow infrastructure or environment, see [CONTRIBUTING.md](./CONTRIBUTING.md).

### Setting up the Dev Environment

The DAGs run on [Amazon MWAA](./mwaa/README.md) with Airflow 3.3.1 and Python 3.12. To develop them, install the same Airflow version and packages in a local Python 3.12 virtual environment, as described in [Local Development](#local-development). From there you can run a DAG with `dag.test()`, or run the scheduler and UI with `airflow standalone`, against a local SQLite database.

Airflow resolves its secrets from AWS Secrets Manager, so you first need AWS credentials.

#### AWS credentials

The Airflow Secrets backend reads connections and variables from AWS Secrets Manager in the `dpe-prod` account. Authenticate with your **own** AWS Identity Center (SSO) credentials — there is no shared IAM user.

**One-time setup:** configure an SSO profile with `aws configure sso` using start URL `https://d-906769aa66.awsapps.com/start`, region `us-east-1`, account `766808016710`, and the `Developer` (or `Administrator`) role.

**Every session:** log in to refresh your SSO token:

```console
aws sso login --profile <your-sso-profile>
```

Then point Airflow at the profile, as described in [3. Configure environment variables](#3-configure-environment-variables).

### Local Development

If you want to test a DAG locally, you can run it directly against a local Airflow SQLite database.

#### 1. Install dependencies

There is a shell script that you can run to install the local Airflow requirements. It installs the Airflow version and packages that MWAA runs, constrained by [`mwaa/constraints.txt`](./mwaa/constraints.txt). Activate a Python 3.12 virtual environment, then run the script. The script stops with an error if `python` is not Python 3.12, or if the `airflow` command on your `PATH` is not the Airflow version that it installed.

```console
bash scripts/install_local_airflow.sh
```

#### 2. Initialize the Airflow database

> [!WARNING]
> **If you used Airflow 2 on this machine before, use a new `AIRFLOW_HOME`.** Airflow 3 cannot use the database and `airflow.cfg` that Airflow 2 left in `~/airflow`. `airflow db migrate` stops at migration 0041, and DAGs fail with `No module named 'airflow.providers.fab'`. Set the new `AIRFLOW_HOME` before `airflow db migrate`:
>
> ```console
> export AIRFLOW_HOME="$HOME/airflow3"
> ```
>
> In step 3, also remove the `#` before `AIRFLOW_HOME` in `.env`.

```console
airflow db migrate
```

This creates the local SQLite metadata database (including the `task_instance` table) that Airflow needs to run `dag.test()`.

#### 3. Configure environment variables

Prior to configuring environment variables, configure and log in to an SSO profile with read access to the `dpe-prod` Secrets Manager secrets (`airflow/connections/`, `airflow/variables/`) — see [AWS credentials](#aws-credentials) for the one-time setup and `aws sso login` step.

[`.env.example`](./.env.example) has the environment variables that Local Development needs. They point Airflow at this repo's `dags/` folder, resolve connections and variables from AWS Secrets Manager with your profile, and let tasks pass custom dataclasses. The comments in the file tell you what each variable does. Copy it to `.env`, then set `AWS_PROFILE` in `.env` to your profile:

```console
cp .env.example .env
```

Using a profile is the recommended way to authenticate: `aws sso login` and anything that talks to AWS directly (e.g. a DAG's S3 upload) pick it up from the environment, and the backend kwargs reference it as `$AWS_PROFILE`, so you only fill in the profile name once.

Load `.env` in each new shell, from the repo root:

```console
set -a; source .env; set +a
```

#### 4. Run a DAG locally

```console
python dags/<your-dag-file>.py
```

DAG files with an `if __name__ == "__main__":` block that calls `dag.test()` can be run directly this way. Any other DAG runs by its DAG id:

```console
airflow dags test <dag_id>
```

`-c '{"<param>": "<value>"}'` overrides the DAG's `Param` values for the run, like `dag.test(run_conf=...)`.

See section [Testing](./CONTRIBUTING.md#testing) in the CONTRIBUTING.md for more info on testing a dag and how you can bypass configuring with AWS.

#### 5. Run the scheduler and UI

`dag.test()` and `airflow dags test` run a DAG without a scheduler. To trigger DAGs from the UI, or to test what the scheduler and DAG processor run (DAG-level callbacks, sensors in `reschedule` mode), start every Airflow component in one process. Load `.env` first, as in [3. Configure environment variables](#3-configure-environment-variables). `AIRFLOW__CORE__DAGS_FOLDER` in it tells Airflow to load the DAGs from this repo:

```console
airflow standalone
```

Open http://localhost:8080 and log in as `admin` with the password printed at startup. It is also saved in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` (`AIRFLOW_HOME` defaults to `~/airflow`). New DAGs are paused; unpause them in the UI or with `airflow dags unpause <dag_id>`.

### Local DAGs

This repository also contains recipes for specific projects that either don't need to be deployed to Airflow or are not ready to be deployed to Airflow. These recipes can be run locally from the `local` directory. Each sub-directory contains recipes specific to a project and those project folders have their own documentation for running the recipes.

The recipes use [py-orca](https://github.com/Sage-Bionetworks-Workflows/py-orca), which pins Airflow 2.10.5 and Python <3.12, so they need their own Python 3.10 environment, separate from the [Local Development](#local-development) one. A convenience script sets it up with the packages in [local/requirements.txt](./local/requirements.txt):

```console
bash local/dev_setup.sh
source venv/bin/activate
```

## Contributing

For detailed contribution guidelines, repository structure, and testing instructions, see [CONTRIBUTING.md](CONTRIBUTING.md).

# ORCA Recipes

This repository contains Airflow recipes (DAGs) for data processing and engineering at Sage Bionetworks. If you want to develop a workflow to process data, you've come to the right place.

## Example Workflows

- **Challenge Automation** - Automatically evaluate challenge submissions by fetching entries from Synapse and orchestrating Nextflow workflows via Seqera Platform.
- **Dataset Discovery** - Generate Croissant-format metadata for Synapse datasets and publish to public S3 for improved discoverability.
- **Analytics Pipelines** - Sync Synapse Portal data to Snowflake and generate platform usage reports tracking downloads, users, and storage.
- **Bioinformatics QC** - Launch and monitor data quality control workflows for genomics projects (GENIE, HTAN).

## Airflow Development

### Setting up the Dev Environment 

A complete Airflow deployment is made up of multiple services running in parallel, so the steps involved in setting up a dev environment are more complex than you may be used to. There are two steps involved in setting up Airflow for development:

1. (Highly recommended) Develop within the provided dev container. A dev container is a virtual machine (e.g., Docker container) that standardizes tools, libraries, and configs for consistent development across machines. This provides us with a consistent environment for the next step.
2. Run docker compose to deploy the full suite of containerized services.

#### Dev Container

There are multiple ways to set up and interface with a dev container, depending on whether you want an IDE-agnostic approach, a VS Code workflow with the Dev Containers extension, or a cloud option like GitHub Codespaces. The cloud option is the most straightforward, and saves us the hassle of configuring Airflow secrets, although because the infrastructure is running in the cloud, there is a limit on how much time we can develop before we need to pay for the service. 

* Note: The environment setup for the Dev Container is defined in [Dockerfile](./Dockerfile). _How_ we deploy the container locally is defined in [devcontainer.json](.devcontainer/devcontainer.json).

##### Codespaces

1. Create a branch for your changes
2. From the main repo page click on `<> Code`
3. Under `Codespaces` click the 3 ellipses and `New with options...`
4. Choose your branch and 4-core (2-core is sufficient for basic edits without docker compose).

##### VS Code

Visual Studio Code provides an extension so that your IDE terminal and other development tools are run within a dev container. Follow the instructions [here](https://code.visualstudio.com/docs/devcontainers/tutorial) to set up the Dev Containers extension. Do not create a new dev container, but rather use the existing configuration by opening the Command Palette (CMD+Shift+p by default on Mac) → "Dev Containers: Reopen in Container."

With this option, you won't be able to use the pre-configured Airflow Secrets as you would in Codespaces. Alternatively, you can [connect to Codespaces as a remote environment from within VS Code](https://docs.github.com/en/codespaces/developing-in-a-codespace/using-github-codespaces-in-visual-studio-code).

#### Docker Compose

Ensure that your Docker installation is up to date (we use [Docker Compose V2](https://docs.docker.com/compose/compose-v2/)). It's recommended that you deploy from within the included dev container (previous section).

We pass environment variables to our build via the `.env` file. We use AWS as our Airflow Secrets backend, although if you are deploying within Codespaces, there's no need to include AWS credentials in the `.env` file since a default IAM user has already been configured in this repository's secrets.

```console
# Duplicate example `.env` file
# Add AWS credentials if you are *not* using Codespaces.
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

Airflow’s webserver listens on port 8080 by default. You can connect in your browser at http://localhost:8080. The username and password will be "airflow".

If you encounter the `nginx bad gateway` errors when navigating to the forwarded port, just wait and refresh a couple of times. Airflow takes a few minutes to become available.

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

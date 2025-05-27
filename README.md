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

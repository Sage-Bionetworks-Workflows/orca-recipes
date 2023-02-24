# ORCA Airflow Recipes

This repository contains Airflow recipes (DAGs) for data processing and engineering at Sage Bionetworks.

## Quick Start

This assumes that you have Docker installed with [Docker Compose V2](https://docs.docker.com/compose/compose-v2/). It's recommended that you leverage the included Dev Container definition (_i.e._ `devcontainer.json`) to standardize your development environment. You can use the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) VS Code extension or GitHub Codespaces.

```console
docker compose up --build --detach
```

After you're done editing your DAG, you can restart the containers so Airflow can pick up on the latest version as follows.

```console
docker compose restart
```

If you edit `Dockerfile`, `docker-compose.yaml`, or `Pipfile`/`Pipfile.lock`, you'll need to rebuild the containers as follows. 

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

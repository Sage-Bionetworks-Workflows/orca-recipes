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

If you edit `Dockerfile`, `docker-compose.yaml`, or `requirements.txt`, you'll need to rebuild the custom container as follows. 

```console
docker compose down
docker compose up --build --detach
```

If you want to update package versions (_e.g._ for `orca`) without needing to update the `requirements.txt` file, you will need to use a special option to avoid cached container image layers.

```console
docker compose down
docker compose build --no-cache
docker compose up --detach
```

**N.B.** For example, if the `requirements.txt` file lists `orca~=1.0` and `orca==1.1` is released, the `~=1.0` version spec will automatically match `==1.1`, so you just need to re-run the `pip install` command. However, as far as Docker is concerned, the `requirements.txt` file hasn't changed, so it normally caches that container image layer. Hence, we need to use `--no-cache` to avoid this behavior.

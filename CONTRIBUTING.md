# Contribution Guidelines

## Infrastructure

We have two Airflow servers:

<!-- Do we need `airflow-dev` if we have DevContainers? -->
1. `airflow-dev`: Hosted in the `dnt-dev` AWS account. Deployed manually. Has a public IP address (accessible using the VPN).
	* Deployed from the `develop` branch in this repository.
2. `airflow-prod`: Hosted in the `dpe-prod` AWS account. Deployed using CloudFormation. Only has a private IP address (accessible via SSM port forwarding).
	* Deployed from the `main` branch in this repository.

There is a helper script in this repository for accessing these Airflow servers.

## Development

To develop on this repository, it's recommended that you use the Dev Containers setup online using GitHub Codespaces. If you must, you can use the VS Code "Dev Containers" extension to develop locally, but this would require you to configure your secrets (_e.g._ using the `.env` file).

## Secrets

Airflow secrets (_e.g._ connections and variables) are stored in the following locations:

<!-- Maybe we can store all secrets (both test and prod) in `dpe-prod`? -->
- `dpe-prod` AWS account: Production-level secrets (_e.g._ actual Tower workspaces)
- `dnt-dev` AWS account: Test-level secrets (_e.g._ `example-project` Tower workspace)

The credentials for a `dnt-dev` AWS service account will be stored in the `orca-recipes-airflow` repository, enabling the AWS Secrets Manager backend to retrieve values from `dnt-dev`.

New secrets will need to be created in both AWS accounts. You can start with the `dnt-dev` account with test credentials. Once the DAG is ready for production, the actual credentials will get stored in `dpe-prod`.

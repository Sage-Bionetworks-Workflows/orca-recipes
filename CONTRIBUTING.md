# Contribution Guidelines

## Infrastructure

We have one Airflow server:

1. `airflow-prod`: Hosted in the `dpe-prod` AWS account. Deployed using CloudFormation. Only has a private IP address (accessible via SSM port forwarding).
	* Deployed from the `main` branch in this repository.

There is a helper script in this repository for accessing this Airflow server.

## Development

To develop on this repository, it's recommended that you use the Dev Containers setup online using GitHub Codespaces. While the entry-level machine type (2-core, 4GB RAM) in Codespaces supports basic editing, you should use a bigger machine type (at least 4-core, 8GB RAM) if you plan on running Airflow using Docker Compose.

Note that you don't need to add your AWS credentials to the `.env` file when using GitHub Codespaces because a default IAM user has been configured in the repository's secrets.

1. Create a branch for your changes
2. From the main repo page click on `<> Code`
3. Under `Codespaces` click the 3 ellipses and `New with options...`
4. Choose your branch and 4-core
5. Once created and you are connected follow the [Quick Start guide](https://github.com/Sage-Bionetworks-Workflows/orca-recipes/blob/main/README.md#quick-start) to start the Airflow server running in the GitHub codespace.
6. After your've ran the docker comamnds you will find the Forwarded Address under the `PORTS` tab that you may use to connect to the airflow UI.

* Note: The instructions above will create a development environment with all necessary dependencies for Airflow development. The environment setup for the Dev Container is defined in `Dockerfile`.

## Testing

Testing should be done via the Dev Containers setup online using GitHub Codespaces. Note that for testing of the DAGs directly on Airflow locally via Dev Containers, it's best to leave the DAG **unpaused** when triggering the DAG with various updates, otherwise you might be triggering the DAG twice and/or triggering it in its original state that had its parameters set to production mode.


## Secrets

Airflow secrets (_e.g._ connections and variables) are stored in the `dpe-prod` AWS account.

The credentials for the `dpe-prod` AWS service account are stored in the `orca-recipes` repository, enabling the AWS Secrets Manager backend to retrieve values from `dpe-prod`.

New secrets only need to be created in the `dpe-prod` account. For connection URIs, the secret name should have the prefix `airflow/connections`. Variables should have the prefix `airflow/variables`.

During DAG development and testing, you can create a secret containing the connection URI (or secret variable) for development resources (such as Nextflow Tower Dev). Once you are ready to run the DAG in production, you can update the secret value with a connection URI for production resources (such as Nextflow Tower Prod).

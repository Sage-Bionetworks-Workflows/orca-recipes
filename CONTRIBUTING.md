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

## DAG Development Best Practices

Follow these best practices when developing DAGs to ensure reliability and maintainability. For comprehensive guidance, refer to the [Airflow Best Practices documentation](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html).

### Communication Between Tasks
* **Treat tasks as transactions** - Tasks should produce the same outcome on every re-run and never produce incomplete results
* **Use XCom for small messages** - For passing small data between tasks in distributed environments
* **Choose appropriate storage for data files** - Use temporary files for small data that can be quickly transferred, and S3 for larger datasets. Pass file paths via XCom rather than storing files locally on workers
* **Store authentication securely** - Use [Airflow Connections](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html) instead of hardcoding passwords or tokens in tasks

### Code Quality
* **Avoid top-level code** - Minimize code outside of operators and DAG definitions to improve scheduler performance and scalability
* **Use local imports** - Import heavy libraries inside task functions rather than at the top level to reduce DAG parsing time

### Testing and Validation
* **Test DAGs in Codespaces first** - Always test new DAGs thoroughly in your GitHub Codespaces development environment before deploying to production
* **Verify task functionality** - Ensure all tasks execute successfully and produce expected outputs in the development environment
* **Validate production deployment** - After deploying to production, monitor initial DAG runs to confirm they function as expected with production data and resources

## Secrets

Airflow secrets (_e.g._ connections and variables) are stored in Secrets Manager within the `dpe-prod` AWS account. This repository uses an IAM User `airflow-secrets-backend` to access the secrets. Access keys for the IAM account are stored in this repository as codespace secrets, enabling Airflow deployments in our configured codespaces environment to retrieve connection URIs and secret variables from `dpe-prod`. The credentials used in the repository's codespace secrets must be rotated manually within the AWS console, and updated every 90 days.

### Creating a new secret

New secrets must be created in AWS Secrets Manager in the `dpe-prod` account. For connection URIs, the secret name should have the prefix `airflow/connections/` 
(i.e. `airflow/connections/MY_SECRET_CONNECTION_STRING`). Variables should have the prefix `airflow/variables/` (i.e. `airflow/variables/MY_SECRET_VARIABLE`).

Within a DAG, you can then use your connection when instantiating a `Hook` [object](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html#hooks), like:

```python
from orca.services.synapse import SynapseHook

syn_hook = SynapseHook("MY_SECRET_CONNECTION_STRING")
```

or your secret variable, like:

```python
my_secret_variable = Variable.get("MY_SECRET_VARIABLE")
```


During DAG development and testing, you can create a secret containing the connection URI (or secret variable) for development resources (such as Nextflow Tower Dev). Once you are ready to run the DAG in production, you can update the secret value with a connection URI for production resources (such as Nextflow Tower Prod).

## Contributing a Challenge DAG

As part of propping up a new Synapse-hosted challenge, the workflow you choose in [nf-synapse-challenge](https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge/tree/main) will need to
be automated to run for challenge submissions on a rolling basis. For this we leverage Airflow DAGs to perform these
automated executions of your customized challenge workflow.

This section describes how you can contribute a new challenge DAG by configuring the `challenge_configs.yaml` file
and the DAG factory pattern in this repository.

----

### TL;DR

To add a DAG for your challenge, make a pull request introducing a profile to the `challenge_configs.yaml` file with the parameters listed in [Creating Your Challenge DAG Config](#creating-your-challenge-dag-config).
See below for a template you can follow.

Modify everything in `<>` and remove the `<>` when complete:

```
<my-challenge>:
  synapse_conn_id: "<MY_SYNAPSE_CONN_ID>"
  aws_conn_id: "<MY_AWS_CONN_ID>"
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

Note that while `synapse_conn_id` and `aws_conn_id` are customizable, we do have connection IDs already in-place
for you to use that will connect you to Synapse (through DPE's ORCA Service Account) and to AWS. But feel free to swap
these for your own if they do not suit your needs.

```
synapse_conn_id: "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN"
aws_conn_id: "AWS_TOWER_PROD_S3_CONN"
```

----

### Overview

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

1. `synapse_conn_id`: The Airflow connection ID used to connect to the Synapse service. This connection is utilized when fetching submission data and updating statuses. **Use `SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN`**.  
1. `aws_conn_id`: The connection ID for AWS. This enables the DAG to upload CSV files (manifests) to an S3 bucket via the S3 hook. **Use `AWS_TOWER_PROD_S3_CONN`**.  
1. `revision`: Specifies the version or Git commit revision of the `main` branch in the `nf-synapse-challenge` repository (which houses your workflow). This ensures that the correct version of your workflow is deployed when the DAG triggers a run. **Use `main` or a specific commit SHA which points to the desired version**.  
1. `challenge_profile`: Identifies the Nextflow Tower challenge profile you contributed in `nextflow.config` of the `nf-synapse-challenge` repository. This parameter customizes the execution environment for the workflow. See example of a previous profile contribution [here](https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge/pull/47). 
1. `tower_conn_id`: The Airflow connection ID to connection URI of the Seqera tower workspace for your challenge. This is needed so your challenge DAG can execute the workflow runs in your desired Seqera workspace.
1. `tower_view_id`: The identifier used to query the submission view on Synapse. It tells the DAG, to _tell the workflow_, where to look for submissions to fetch and process.  
1. `tower_compute_env_type`: Indicates the compute environment (for example, `"spot"`) to be used when launching the workflow. **Use `spot` for challenges that will take less computational time to evaluate the submissions. Use `on-demand` otherwise**.  
1. `bucket_name`: The S3 bucket where the challenge-related files (such as CSV manifests) will be stored. Note that the Seqera Platform workspaces can only access the S3 buckets assigned to them.
1. `key`: The S3 key prefix (or folder path) under which the submissions manifest file is uploaded for a challenge DAG run. At runtime, a unique run-specific UUID is appended to this key to ensure that files are uniquely identified and organized. Since this folder path lives in a scratch bucket, you can leverage one of the folders that are configured to delete stale objects based on a certain number of days ([see here](https://sagebionetworks.jira.com/wiki/spaces/WF/pages/2191556616/Getting+Started+with+Nextflow+and+Seqera+Platform#Tower-Project-Breakdown) for more details). This will affect what value you put here. For example, **if you would like for your manifest file to live for 10 days, use `10days/my_project_folder`**.  
1. `dag_config`: A nested dictionary containing additional DAG scheduling and runtime parameters:  
   * `schedule_interval`: A cron expression that determines how frequently the DAG is triggered.  
   * `start_date`: An ISO-formatted date string that specifies when the DAG should start running. The DAG factory converts this to a Python datetime object.  
   * `catchup`: A Boolean flag that indicates whether Airflow should run missed DAG runs (catch up) if the scheduler falls behind.  
   * `default_args`: Standard Airflow arguments for the DAG. For example, you can set the number of retries for tasks.  
   * `tags`: A list of tags for categorizing the DAG in the Airflow UI.


### Contributing a New Challenge DAG

To contribute a new challenge:

1. **Add/Update Configuration**: Create a new section in `challenge_configs.yaml` with a unique challenge key. Provide all required parameters as described above.
1. **Validate the Parameters**: Make sure all connection IDs, S3 bucket names, and the revision match your environment and the workflow you intend to run.
1. **Submit a Pull Request**: Make a pull request to submit your changes. A repository maintainer will do a revision and work with you in the next step.
1. **Test in Your Environment**: Validate the new configuration by running your DAG in a codespace environment before merging and deploying to production.
1. **Merge your Pull Request**: Once your changes have been tested, your PR is ready for merge and a maintainer will ensure your DAG is created and running in Airflow production!

By following these guidelines, you will successfully contribute a deployable challenge DAG customized for your needs.

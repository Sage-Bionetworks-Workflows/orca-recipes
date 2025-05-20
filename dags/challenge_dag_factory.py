import io
import os
import requests
import uuid
from datetime import datetime
import yaml
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Param, DagRun
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.session import create_session
from airflow.utils.state import State

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook

# Define the path to your challenge configuration file.
CONFIG_URL = "https://raw.githubusercontent.com/Sage-Bionetworks-Workflows/orca-recipes/main/dags/challenge_configs.yaml"


def load_challenge_configs(url=CONFIG_URL):
    """Load challenge configurations from a raw GitHub URL."""

    response = requests.get(url, timeout=10)

    if not response.ok:
            raise RuntimeError(
                f"Failed to fetch challenge configs... "
                f"Status code: {response.status_code}, reason: {response.reason}"
            )

    return yaml.safe_load(response.text)

def resolve_dag_config(challenge_name: str, dag_params: dict, config: dict) -> dict:
    """
    Return the DAG configuration for a challenge.

    If the challenge configuration provides a custom `dag_config`, use it
    (ensuring any ISO-format dates are converted and that task parameters are injected).
    Otherwise, return the default DAG configuration.

    Arguments:
        challenge_name: The name of the challenge.
        dag_params: A dictionary of DAG parameters.
        config: The challenge configuration.

    Returns:
        dict: The resolved DAG configuration.
    """
    
    # Start with default configuration
    dag_config = {
        "schedule_interval": "*/3 * * * *",
        "start_date": datetime(2024, 4, 9),
        "catchup": False,
        "default_args": {"retries": 2},
        "tags": ["nextflow_tower"],
        "params": dag_params,
    }

    # Update with any custom configuration if provided
    if config.get('dag_config'):
        dag_config.update(config['dag_config'])
        
        # Ensure start_date is a datetime object if provided
        if 'start_date' in dag_config and isinstance(dag_config['start_date'], str):
            dag_config['start_date'] = datetime.fromisoformat(dag_config['start_date'])
            
        # Ensure challenge name is in tags
        if 'tags' in dag_config:
            if challenge_name not in dag_config['tags']:
                dag_config['tags'].append(challenge_name)
        else:
            dag_config['tags'] = [challenge_name]

    return dag_config

def get_processed_submission_ids(dag_id: str, n_runs: int = 6) -> set:
    """
    Queries the last `n_runs` of successful or running DagRuns for `dag_id`,
    and pulls their XComs for `get_new_submissions`, returning a set of all submission IDs seen.

    Tip:
        When the same submission ID needs to be evaluated for testing purposes,
        set `n_runs` to 0.

    Arguments:
        dag_id: The ID of the DAG to query.
        n_runs: The number of DagRuns to query.

    Returns:
        set: A set of all submission IDs seen.

    """

    processed_submissions = set()

    with create_session() as session:

        # Retrieve the last `n_runs` of successful or running DagRuns
        previous_runs = (
            session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.state.in_([State.SUCCESS, State.RUNNING])
            ).order_by(DagRun.start_date.desc()).limit(n_runs).all()
        )

        # Retrieve the task runs for the task of interest
        task_id = "get_new_submissions"
        task_runs = [run.get_task_instance(task_id) for run in previous_runs]

        # Now retrieve the submission IDs for each run of `get_new_submissions` task
        for task_run in task_runs:

            # Query the metadata DB (``xcom_pull``) to retrieve the submission IDs...
            # By default, pulling the XComs for `get_new_submissions`
            # will return the submission ID list, since that is the return value
            prev_subs = task_run.xcom_pull(task_ids=task_id)

            print(f"Previous submission IDs: {prev_subs}")

            if prev_subs:
                processed_submissions.update(prev_subs)

    return processed_submissions

def create_challenge_dag(challenge_name: str, config: dict):

    # Define parameters for the DAG, including new per-challenge settings.
    dag_params = {
        "synapse_conn_id": Param(config["synapse_conn_id"], type="string"),
        "aws_conn_id": Param(config["aws_conn_id"], type="string"),
        "revision": Param(config["revision"], type="string"),
        "challenge_profile": Param(config["challenge_profile"], type="string"),
        "tower_conn_id": Param(config["tower_conn_id"], type="string"),
        "tower_view_id": Param(config["tower_view_id"], type="string"),
        "tower_compute_env_type": Param(config["tower_compute_env_type"], type="string"),
        "bucket_name": Param(config["bucket_name"], type="string"),
        "key": Param(config["key"], type="string"),
    }

    # Resolve the complete DAG configuration.
    dag_config = resolve_dag_config(challenge_name, dag_params, config)

    # Create a unique DAG ID for the challenge.
    dag_id = f"{challenge_name}_challenge_dag"

    @dag(dag_id=dag_id, **dag_config)
    def challenge_dag():

        @task
        def generate_run_uuid():
            return str(uuid.uuid4())

        @task(do_xcom_push=False)
        def verify_bucket_name(**context):
            hook = NextflowTowerHook(context["params"]["tower_conn_id"])
            workspace = os.path.basename(hook.ops.workspace)

            bucket_name = context["params"]["bucket_name"].lower()
            expected_bucket_names = [f"{workspace}{suffix}".lower() for suffix in ["-tower-scratch", "-tower-bucket"]]

            if bucket_name not in expected_bucket_names:
                raise ValueError(f"Invalid bucket name: {bucket_name}. Expected one of {expected_bucket_names}")

        @task
        def get_new_submissions(**context):
            hook = SynapseHook(context["params"]["synapse_conn_id"])
            submissions = hook.ops.get_submissions_with_status(
                context["params"]["tower_view_id"], "RECEIVED"
            )

            dag_id = context["dag_run"].dag_id
            processed = get_processed_submission_ids(dag_id=dag_id)

            # Filter out previously-retrieved submissions
            new_submissions = [s for s in submissions if s not in processed]

            return new_submissions

        @task.branch()
        def update_submission_statuses(submissions, **context):
            if submissions:
                hook = SynapseHook(context["params"]["synapse_conn_id"])
                for submission in submissions:
                    hook.ops.update_submission_status(
                        submission_id=submission, submission_status="EVALUATION_IN_PROGRESS"
                    )
                return "stage_submissions_manifest"
            return "stop_dag"

        @task
        def stop_dag():
            # A dummy task to mark the end when there are no submissions.
            pass

        @task
        def stage_submissions_manifest(submissions, run_uuid, **context):
            # Use per-challenge bucket and key settings from the DAG params.
            bucket_name = context["params"]["bucket_name"]
            key_root = context["params"]["key"]
            s3_hook = S3Hook(
                aws_conn_id=context["params"]["aws_conn_id"],
                region_name="us-east-1"  # You could also make region configurable.
            )
            # Create a CSV manifest from submissions.
            df = pd.DataFrame({"submission_id": submissions})

            # Create a text buffer to hold the CSV data in-memory
            text_buffer = io.StringIO()

            # Write the data into the buffer
            df.to_csv(text_buffer, index=False)

            # Get the data from the buffer
            submissions_content = text_buffer.getvalue()

            # Encode the data to bytes for the s3 upload
            submissions_bytes = submissions_content.encode("utf-8")
            
            # Create a bytes bufferobject
            bytes_buffer = io.BytesIO(submissions_bytes)

            # Create a key path that incorporates the unique run identifier
            s3_key = f"{key_root}/{run_uuid}/submissions.csv"

            # Upload the file
            s3_hook.load_file_obj(file_obj=bytes_buffer,
                                  key=s3_key,
                                  bucket_name=bucket_name,
                                  replace=True)

            # Return S3 URI.
            return f"s3://{bucket_name}/{s3_key}"

        @task
        def launch_workflow(manifest_path, run_uuid, **context):
            hook = NextflowTowerHook(context["params"]["tower_conn_id"])
            info = LaunchInfo(
                run_name=f"{challenge_name}-evaluation-{run_uuid}",
                pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge",
                revision=context["params"]["revision"],
                workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
                profiles=["tower", context["params"]["challenge_profile"]],
                params={"manifest": manifest_path},
            )
            tower_run_id = hook.ops.launch_workflow(info, context["params"]["tower_compute_env_type"])
            return tower_run_id

        @task.sensor(poke_interval=60, timeout=604800, mode="reschedule")
        def monitor_workflow(tower_run_id, **context):
            hook = NextflowTowerHook(context["params"]["tower_conn_id"])
            workflow = hook.ops.get_workflow(tower_run_id)
            print(f"Current workflow state: {workflow.status.state.value}")
            return workflow.status.is_done

        # Set up task dependencies and declare the order of execution
        verify = verify_bucket_name()
        run_uuid = generate_run_uuid()
        submissions = get_new_submissions()
        submissions_updated = update_submission_statuses(submissions)
        stop = stop_dag()
        manifest_path = stage_submissions_manifest(submissions, run_uuid)
        tower_run_id = launch_workflow(manifest_path, run_uuid)
        monitor = monitor_workflow(tower_run_id)

        # Fail fast if ``bucket_name`` is invalid
        verify >> submissions >> submissions_updated >> [stop, manifest_path]
        manifest_path >> tower_run_id >> monitor

    return challenge_dag()


# Load configurations and generate DAGs for each challenge
challenge_configs = load_challenge_configs()
for challenge_name, config in challenge_configs.items():
    # This assigns each created DAG to the module-level globals so that Airflow can detect them
    globals()[f"{challenge_name}_challenge_dag"] = create_challenge_dag(challenge_name, config)

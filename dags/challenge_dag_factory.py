import os
import uuid
from datetime import datetime
import yaml
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook

# Define the path to your challenge configuration file.
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "challenge_configs.yaml")


def load_challenge_configs():
    """Load challenge configurations from a YAML file."""
    with open(CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)

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
    if 'dag_config' in config and config['dag_config']:
        custom_config = config['dag_config'].copy()
        if 'start_date' in custom_config:
            # Convert a start_date provided as an ISO format string to a datetime object.
            custom_config['start_date'] = datetime.fromisoformat(custom_config['start_date'])
        # Always ensure that task parameters are included.
        custom_config['params'] = dag_params
        if 'tags' not in custom_config:
            custom_config['tags'] = [challenge_name]
        return custom_config
    else:
        return {
            "schedule_interval": "*/1 * * * *",
            "start_date": datetime(2024, 4, 9),
            "catchup": False,
            "default_args": {"retries": 2},
            "tags": ["nextflow_tower"],
            "params": dag_params,
        }

def create_challenge_dag(challenge_name: str, config: dict):
    # Generate a new uuid if none is provided.
    if not config.get("uuid"):
        config["uuid"] = str(uuid.uuid4())

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
        "uuid": Param(config["uuid"], type="string"),
    }

    # Resolve the complete DAG configuration.
    dag_config = resolve_dag_config(challenge_name, dag_params, config)

    # Create a unique DAG ID for the challenge.
    dag_id = f"{challenge_name}_challenge_dag"

    @dag(dag_id=dag_id, **dag_config)
    def challenge_dag():
        @task
        def get_new_submissions(**context):
            hook = SynapseHook(context["params"]["synapse_conn_id"])
            submissions = hook.ops.get_submissions_with_status(
                context["params"]["tower_view_id"], "RECEIVED"
            )
            return submissions

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
        def stage_submissions_manifest(submissions, **context):
            # Use per-challenge bucket and key settings from the DAG params.
            bucket_name = context["params"]["bucket_name"]
            key_root = context["params"]["key"]
            s3_hook = S3Hook(
                aws_conn_id=context["params"]["aws_conn_id"],
                region_name="us-east-1"  # You could also make region configurable.
            )
            # Create a CSV manifest from submissions.
            df = pd.DataFrame({"submission_id": submissions})
            csv_file = "submissions.csv"
            df.to_csv(csv_file, index=False)
            run_uuid = context["params"]["uuid"]
            # Create a key path that incorporates the unique run identifier.
            s3_key = f"{key_root}/{run_uuid}/{csv_file}"
            # Upload the file.
            s3_hook.load_file(filename=csv_file, key=s3_key, bucket_name=bucket_name)
            os.remove(csv_file)
            # Return S3 URI.
            return f"s3://{bucket_name}/{s3_key}"

        @task
        def launch_workflow(manifest_path, **context):
            hook = NextflowTowerHook(context["params"]["tower_conn_id"])
            run_uuid = context["params"]["uuid"]
            info = LaunchInfo(
                run_name=f"{challenge_name}-evaluation-{run_uuid}",
                pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge",
                revision=context["params"]["revision"],
                workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
                profiles=["tower", context["params"]["challenge_profile"]],
                params={"manifest": manifest_path},
            )
            run_id = hook.ops.launch_workflow(info, context["params"]["tower_compute_env_type"])
            return run_id

        @task.sensor(poke_interval=60, timeout=604800, mode="reschedule")
        def monitor_workflow(run_id, **context):
            hook = NextflowTowerHook(context["params"]["tower_conn_id"])
            workflow = hook.ops.get_workflow(run_id)
            print(f"Current workflow state: {workflow.status.state.value}")
            return workflow.status.is_done

        # Set up task dependencies.
        submissions = get_new_submissions()
        submissions_updated = update_submission_statuses(submissions)
        stop = stop_dag()
        manifest_path = stage_submissions_manifest(submissions)
        run_id = launch_workflow(manifest_path)
        monitor = monitor_workflow(run_id)

        submissions >> submissions_updated >> [stop, manifest_path]
        manifest_path >> run_id >> monitor

    return challenge_dag()


# Load configurations and generate DAGs for each challenge
challenge_configs = load_challenge_configs()
for challenge_name, config in challenge_configs.items():
    # This assigns each created DAG to the module-level globals so that Airflow can detect them
    globals()[f"{challenge_name}_challenge_dag"] = create_challenge_dag(challenge_name, config)

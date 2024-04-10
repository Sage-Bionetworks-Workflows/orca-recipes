import uuid
import os
from datetime import datetime
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook

REGION_NAME = "us-east-1"
BUCKET_NAME = "pegs-challenge-project-tower-scratch"
FILE_NAME = f"submissions_{uuid.uuid4()}.csv"
KEY = "work"

dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "synapse_evaluation_id": Param("9615511", type="string"),
    "aws_conn_id": Param("AWS_TOWER_PROD_S3_CONN", type="string"),
    "tower_conn_id": Param("PEGS_CHALLENGE_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param("pegs_model_submission_evaluation", type="string"),
    "tower_view_id": Param("syn55253884", type="string"),
    "tower_input_id": Param("syn53239289", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
}

dag_config = {
    "schedule_interval": "*/1 * * * *",
    "start_date": datetime(2024, 4, 9),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["nextflow_tower"],
    "params": dag_params,
}


@dag(**dag_config)
def pegs_challenge_submission_dag():
    @task
    def get_new_submissions(**context) -> list[int]:
        hook = SynapseHook(context["params"]["synapse_conn_id"])
        submissions = hook.ops.get_submissions_with_status(
            context["params"]["view_id"], "RECEIVED"
        )
        return submissions

    @task.branch()
    def update_submission_statuses(submissions: list, **context) -> str:
        if submissions:
            hook = SynapseHook(context["params"]["synapse_conn_id"])
            for submission in submissions:
                hook.ops.update_submission_status(
                    submission_id=submission,
                    submission_status="EVALUATION_IN_PROGRESS",
                )
            return "stage_submissions_manifest"
        return "stop_dag"

    @task()
    def stop_dag():
        pass

    @task()
    def stage_submissions_manifest(submissions: list, **context) -> str:
        s3_hook = S3Hook(
            aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME
        )
        df = pd.DataFrame({"submission_id": submissions})
        df.to_csv(FILE_NAME, index=False)
        s3_hook.load_file(
            filename=FILE_NAME, key=f"{KEY}/{FILE_NAME}", bucket_name=BUCKET_NAME
        )
        os.remove(FILE_NAME)
        return f"s3://{BUCKET_NAME}/{KEY}/{FILE_NAME}"

    @task()
    def launch_model2data_workflow(**context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name=context["params"]["tower_run_name"],
            pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge",
            revision="main",
            entry_name="MODEL_TO_DATA_CHALLENGE",
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            params={
                "view_id": context["params"]["tower_view_id"],
                "input_id": context["params"]["tower_input_id"],
            },
        )
        run_id = hook.ops.launch_workflow(
            info, context["params"]["tower_compute_env_type"]
        )
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_model2data_workflow(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    submissions = get_new_submissions()
    submissions_updated = update_submission_statuses(submissions=submissions)
    stop = stop_dag()
    manifest_path = stage_submissions_manifest(submissions=submissions)
    run_id = launch_model2data_workflow()
    monitor = monitor_model2data_workflow(run_id=run_id)

    submissions >> submissions_updated >> [stop, manifest_path]
    manifest_path >> run_id >> monitor


pegs_challenge_submission_dag()
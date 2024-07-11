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
BUCKET_NAME = "dynamic-challenge-project-tower-scratch"
FILE_NAME = "submissions.csv"
KEY = "10days/dynamic_challenge"

dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "aws_conn_id": Param("AWS_TOWER_PROD_S3_CONN", type="string"),
    "tower_conn_id": Param("DYNAMIC_CHALLENGE_PROJECT_TOWER_CONN", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
    "revision": Param("efef73e75dc57a60451746ba56fceba550fa6162", type="string"),
    "challenge_profile": Param("dynamic_challenge", type="string"),
    "view_id": Param("syn52658661", type="string"),
    "send_email": Param(True, type="boolean"),
    "email_with_score": Param("yes", type="string"),
    "uuid": Param(str(uuid.uuid4()), type="string"),
}

dag_config = {
    "schedule_interval": "* * * * *",
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["nextflow_tower"],
    "params": dag_params,
}


@dag(**dag_config)
def dynamic_challenge_dag():
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
    def stop_dag() -> None:
        pass

    @task()
    def stage_submissions_manifest(submissions: list, **context) -> str:
        s3_hook = S3Hook(
            aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME
        )
        df = pd.DataFrame({"submission_id": submissions})
        df.to_csv(FILE_NAME, index=False)
        run_uuid = context["params"]["uuid"]
        s3_hook.load_file(
            filename=FILE_NAME, key=f"{KEY}/{run_uuid}/{FILE_NAME}", bucket_name=BUCKET_NAME
        )
        os.remove(FILE_NAME)
        return f"s3://{BUCKET_NAME}/{KEY}/{run_uuid}/{FILE_NAME}"

    @task()
    def launch_data_to_model_on_tower(manifest_path: str, **context) -> str:
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        run_uuid = context["params"]["uuid"]
        info = LaunchInfo(
            run_name=f"dynamic-challenge-evaluation-{run_uuid}",
            pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge",
            revision=context["params"]["revision"],
            profiles=["tower", context["params"]["challenge_profile"]],
            entry_name="DATA_TO_MODEL_CHALLENGE",
            params={
                "manifest": manifest_path,
                "view_id": context["params"]["view_id"],
                "send_email": context["params"]["send_email"],
                "email_with_score": context["params"]["email_with_score"],
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )
        run_id = hook.ops.launch_workflow(
            info, context["params"]["tower_compute_env_type"]
        )
        return run_id

    @task.sensor(poke_interval=60, timeout=604800, mode="reschedule")
    def monitor_workflow(run_id: str, **context) -> bool:
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    submissions = get_new_submissions()
    submissions_updated = update_submission_statuses(submissions=submissions)
    stop = stop_dag()
    manifest_path = stage_submissions_manifest(submissions=submissions)
    run_id = launch_data_to_model_on_tower(manifest_path=manifest_path)
    monitor = monitor_workflow(run_id=run_id)

    submissions >> submissions_updated >> [stop, manifest_path]
    manifest_path >> run_id >> monitor


dynamic_challenge_dag()

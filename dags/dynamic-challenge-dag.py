import uuid
from datetime import datetime

import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook

UUID = uuid.uuid4()
REGION_NAME = "us-east-1"
BUCKET_NAME = "dynamic-challenge-project-tower-scratch"
FILE_NAME = f"submissions_{UUID}.csv"
KEY = "10days/dynamic_challenge"


dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "aws_conn_id": Param("AWS_TOWER_PROD_S3_CONN", type="string"),
    "tower_conn_id": Param("DYNAMIC_CHALLENGE_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param(f"dynamic-challenge-evaluation_{UUID}", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
    "view_id": Param("syn52658661", type="string"),
    "testing_data": Param("syn53627077", type="string"),
    "scoring_script": Param("dynamic_challenge_score.py", type="string"),
    "validation_script": Param("dynamic_challenge_validate.py", type="string"),    
    "email_with_score": Param("yes", type="string"),
    "synapse_evaluation_id": Param("9615537", type="string"),
}

dag_config = {
    # Run every 2 minutes
    "schedule_interval": "*/2 * * * *",
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
    def get_new_submissions(**context):
        hook = SynapseHook(context["params"]["synapse_conn_id"])
        submissions = hook.ops.get_submissions_with_status(
            context["params"]["view_id"], "RECEIVED"
        )
        return submissions


    @task.branch()
    def update_submission_statuses(submissions: list, **context):
        hook = SynapseHook(context["params"]["synapse_conn_id"])
        for submission in submissions:
            hook.ops.update_submission_status(
                submission_id=submission,
                submission_status="EVALUATION_IN_PROGRESS",
            )
        if submissions:
            return "stage_submissions_manifest"
        return "stop_dag"
    
    @task()
    def stop_dag():
        pass

    @task()
    def stage_submissions_manifest(submissions: list, **context):
        s3_hook = S3Hook(
            aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME
        )
        df = pd.DataFrame({"submission_id": submissions})
        df.to_csv(FILE_NAME, index=False)
        s3_hook.load_file(
            filename=FILE_NAME, key=f"{KEY}/{FILE_NAME}", bucket_name=BUCKET_NAME
        )
        return f"s3://{BUCKET_NAME}/{KEY}/{FILE_NAME}"

    @task()
    def launch_data_to_model_on_tower(manifest_path: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name=context["params"]["tower_run_name"],
            pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge",
            revision="main",
            profiles=["tower"],
            entry_name="DATA_TO_MODEL_CHALLENGE",
            params={
                "manifest": manifest_path,
                "view_id": context["params"]["view_id"],
                "scoring_script": context["params"]["scoring_script"],
                "validation_script": context["params"]["validation_script"],
                "testing_data": context["params"]["testing_data"],
                "email_with_score": context["params"]["email_with_score"],
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )
        run_id = hook.ops.launch_workflow(
            info, context["params"]["tower_compute_env_type"]
        )
        return run_id

    @task.sensor(poke_interval=60, timeout=604800, mode="reschedule")
    def monitor_workflow(run_id: str, **context):
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

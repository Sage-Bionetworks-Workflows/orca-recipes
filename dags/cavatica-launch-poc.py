from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from sagetasks.sevenbridges.utils import SbgUtils
from sagetasks.sevenbridges.general import (
    bundle_client_args,
    get_project_id,
)


DAG_CONFIG = {
    "schedule_interval": None,
    "start_date": datetime(2023, 1, 11),
    "catchup": False,
    "default_args": {
        "retries": 3,
    },
    "tags": ["cavatica"],
}


DAG_PARAMS = {
    "project_name": "include-sandbox",
    "billing_group_name": "include-dev",
    "app_id": "bgrande/include-sandbox/kfdrc-rnaseq-workflow-3",
}


CLIENT_ARGS = bundle_client_args(
    auth_token=Variable.get("SB_AUTH_TOKEN"),
    endpoint=Variable.get("SB_API_ENDPOINT")
)


@dag(**DAG_CONFIG)
def cavatica_launch_poc():

    @task()
    def get_project():
        project_name = DAG_PARAMS["project_name"]
        billing_group_name = DAG_PARAMS["billing_group_name"]
        project_id = get_project_id(CLIENT_ARGS, project_name, billing_group_name)
        return project_id

    @task()
    def prepare_task(project_id: str):
        app_id = DAG_PARAMS["app_id"]
        utils = SbgUtils(CLIENT_ARGS)
        utils.open_project(project_id)

        task_inputs = {
            "input_type": "FASTQ",
            'reads1': utils.client.files.get("62d99af7074179790775fda5"),
            'reads2': utils.client.files.get("62d99afe074179790775fda9"),
            "runThreadN": 36,
            "wf_strand_param": "default",
            "sample_name": "HCC1187_1M",
            "rmats_read_length": 101,
            "outSAMattrRGline": "ID:HCC1187_1M\tLB:Not_Reported\tPL:Illumina\tSM:HCC1187_1M",
            "output_basename": "cavatica_launch_poc"
        }

        task = utils.get_or_create_task(app_id, task_inputs, "cavatica_launch_poc")
        return task.name

    @task()
    def launch_task(project_id: str, task_name: str):
        utils = SbgUtils(CLIENT_ARGS)
        utils.open_project(project_id)
        tasks = utils.get_task(task_name)
        for task in tasks:
            task.run()

    # Explicit dependencies
    project_id = get_project()
    task_name = prepare_task(project_id)
    launch_task(project_id, task_name)


cavatica_launch_poc()

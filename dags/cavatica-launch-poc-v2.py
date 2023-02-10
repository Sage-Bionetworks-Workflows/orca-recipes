from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.sensors.base import PokeReturnValue
from orca.services.sevenbridges import SevenBridgesHook

params = {
    "conn_id": Param("cavatica_test", type="string"),
    "app_id": Param("orca-service/test-project/kfdrc-rnaseq-workflow", type="string"),
}

dag_args: dict[str, Any]
dag_args = {
    "schedule_interval": None,
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "default_args": {
        "retries": 3,
    },
    "tags": ["cavatica"],
    "params": params,
}


@dag(**dag_args)
def cavatica_launch_poc_v2():
    @task
    def create_task(params=None, run_id=None):
        hook = SevenBridgesHook(params["conn_id"])
        task_inputs = {
            "input_type": "FASTQ",
            "reads1": hook.client.files.get("63e569217a0654635c558c84"),
            "reads2": hook.client.files.get("63e5694ebfc712185ac37a27"),
            "runThreadN": 36,
            "wf_strand_param": "default",
            "sample_name": "HCC1187_1M",
            "rmats_read_length": 101,
            "outSAMattrRGline": "ID:HCC1187_1M\tLB:Not_Reported\tPL:Illumina\tSM:HCC1187_1M",
            "output_basename": "cavatica_launch_poc_v2",
        }
        clean_run_id = run_id.replace("+00:00", "Z").replace(":", ".")
        task_id = hook.ops.create_task(clean_run_id, params["app_id"], task_inputs)
        return task_id

    # TODO: In practice, use a longer interval (5 min) in "reschedule" mode
    @task.sensor(poke_interval=10, timeout=604800, mode="poke")
    def monitor_task(task_name):
        # TODO: Once the following PR is merged, use `params` argument
        # (like `create_task()`) instead of `get_current_context()`.
        #   Open PR: https://github.com/apache/airflow/pull/29146
        # Using this approach for retrieving the context/params because
        # @task.sensor don't yet support template variables as arguments
        #   Issue: https://github.com/apache/airflow/issues/29137
        context = get_current_context()
        hook = SevenBridgesHook(context["params"]["conn_id"])
        task_status, is_done = hook.ops.get_task_status(task_name)
        return PokeReturnValue(is_done, task_status)

    @task
    def the_end(task_final_status) -> None:
        print(task_final_status)

    task_id = create_task()
    task_status = monitor_task(task_id)
    the_end(task_status)


cavatica_launch_poc_v2()

from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param
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
            "output_basename": run_id,
        }
        task_id = hook.ops.create_task(run_id, params["app_id"], task_inputs)
        return task_id

    @task.sensor(poke_interval=60, timeout=604800, mode="reschedule")
    def monitor_task(task_name, params=None):
        hook = SevenBridgesHook(params["conn_id"])
        task_status, is_done = hook.ops.get_task_status(task_name)
        return PokeReturnValue(is_done, task_status)

    task_id = create_task()
    task_final_status = monitor_task(task_id)


cavatica_launch_poc_v2()

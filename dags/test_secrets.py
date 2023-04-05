import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from orca.services.sevenbridges import SevenBridgesHook

dag_args: dict[str, Any]
dag_args = {
    "schedule": None,
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}


@dag(**dag_args)
def test_secrets():
    @task
    def check_task():
        hook = SevenBridgesHook("cavatica_test")
        hook.ops.get_task_status("4c258aba-8d62-4912-8746-631d76af5b25")

    @task
    def print_env():
        print(os.environ)

    status = check_task()
    env = print_env()
    env >> status


test_secrets()

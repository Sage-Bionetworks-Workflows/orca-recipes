"""Sensor reschedule/poke_interval demo using a simulated long-running task.

`long_running_task` blocks for RUN_DURATION_SECONDS (standing in for a real
hours-long external job), and `monitor_long_run_task` (mode="reschedule")
polls its TaskInstance state every `poke_interval` seconds until it succeeds.

DO NOT RUN THIS LOCALLY via `python3 tests/integration/test_poll.py` /
`dag.test()`. `dag.test()` uses Airflow's DebugExecutor, which runs tasks one
at a time in a single process/thread. Once `long_running_task` starts its
blocking sleep, it freezes that entire process for the full
RUN_DURATION_SECONDS (a couple hours) — the sensor can't be rescheduled and
re-poked until the sleep finishes, regardless of `poke_interval`, so locally
this just hangs your terminal for hours with no visible progress.

This DAG is meant to be deployed and triggered on a real Airflow deployment
(e.g. MWAA environment) instead, where each task instance runs in its
own separate worker process/pod, so the blocking sleep in `long_running_task`
can't starve the scheduler from re-queuing `monitor_long_run_task` every
`poke_interval` seconds as intended.
"""
import _bootstrap  # noqa: F401  (sets up sys.path for dags.*/src.* imports)
import time
from airflow.decorators import dag, task
from airflow.models import Param

RUN_DURATION_SECONDS = 2 * 60 * 60  # simulate a couple hours of "work"

dag_config = {
    "schedule": None,
}

@dag(**dag_config)
def poll_test_dag():
    @task()
    def long_running_task():
        """Simulate an hours-long task by sleeping for RUN_DURATION_SECONDS."""
        time.sleep(RUN_DURATION_SECONDS)
        return True

    @task.sensor(poke_interval=30, timeout=604800, mode="reschedule")
    def monitor_long_run_task(target_task_id: str,  **context):
        """Monitor the long-running task until it completes."""
        ti = context["dag_run"].get_task_instance(target_task_id)
        print(f"Now tracking task with ID: {target_task_id}")
        return ti.state == "success"

    # 1. Instantiate the long-running task
    long_task = long_running_task()

    # 2. Extract the task_id string dynamically and pass it to the sensor
    monitor = monitor_long_run_task(target_task_id=long_task.operator.task_id)

dag = poll_test_dag()

if __name__ == "__main__":
    dag.test()

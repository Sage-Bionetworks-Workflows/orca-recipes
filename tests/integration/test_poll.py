"""Sensor reschedule/poke_interval demo using a simulated long-running task.

`long_running_task` blocks for a fixed duration (standing in for a real
long-running external job), and `monitor_long_run_task` (mode="reschedule")
polls its TaskInstance state every `poke_interval` seconds until it succeeds.

CAUTION: This does NOT demonstrate real polling behavior when run locally via
`dag.test()`. `dag.test()` uses Airflow's DebugExecutor, which runs tasks one
at a time in a single process/thread. Once `long_running_task` starts its
blocking sleep, it freezes that entire process, so the sensor can't be
rescheduled and re-poked until the sleep finishes, regardless of
`poke_interval`. Real poking on this interval only happens on a real Airflow
deployment (e.g. our EKS-hosted Airflow), where each task instance runs in
its own worker process/pod, so a blocking task can't starve the scheduler
from re-queuing a reschedule-mode sensor.

Run directly with: python3 tests/integration/test_poll.py
"""
import _bootstrap  # noqa: F401  (sets up sys.path for dags.*/src.* imports)
import time
from airflow.decorators import dag, task
from airflow.models import Param


dag_config = {
    "schedule": None,
}

@dag(**dag_config)
def poll_test_dag():
    @task()
    def long_running_task():
        """Simulate a long-running task by sleeping for 10 seconds."""
        time.sleep(60)
        return True

    @task.sensor(poke_interval=5, timeout=604800, mode="reschedule")
    def monitor_long_run_task(target_task_id: str,  **context):
        """Monitor the long-running task until it completes."""
        ti = context["dag_run"].get_task_instance(target_task_id)
        print(f"Now tracking task with ID: {target_task_id}")
        return ti.state == "success"

    # 1. Instantiate the long-running task
    long_task = long_running_task()

    # 2. Extract the task_id string dynamically and pass it to the sensor
    monitor = monitor_long_run_task(target_task_id=long_task.operator.task_id)

    long_task >> monitor

dag = poll_test_dag()

if __name__ == "__main__":
    dag.test()
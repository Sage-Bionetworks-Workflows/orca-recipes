"""Sensor reschedule/poke_interval demo.

A minimal, standalone model of polling an external resource (e.g. a Synapse
entity's state) every `poke_interval` seconds until it changes, then
stopping. `timeout` caps how long it's willing to keep polling before giving
up.

Here, the "external state" being polled is simply enough wall-clock time
having passed since the DAG run started (RUN_DURATION_SECONDS), checked
directly from the sensor's own context — no companion task, global variable,
or XCom needed. A real usage would replace that check with an actual external
call (e.g. fetching a Synapse entity's status).
"""
from datetime import datetime, timezone

from airflow.decorators import dag, task

RUN_DURATION_SECONDS = 2 * 60 * 60  # simulate polling for a couple hours

dag_config = {
    "schedule": None,
}

@dag(**dag_config)
def poll_test_dag():
    @task.sensor(poke_interval=30, timeout=RUN_DURATION_SECONDS, mode="reschedule")
    def monitor_long_run_task(**context):
        """Poll a simulated external resource until its state "changes".

        Stands in for polling something like a Synapse entity's state every
        `poke_interval` seconds until it changes. Here, the "state change" is
        simply enough wall-clock time having passed since the DAG run
        started.

        Returns:
            bool: True once RUN_DURATION_SECONDS has elapsed since the DAG
                run started, signaling the sensor to stop poking.
        """
        elapsed = (
            datetime.now(timezone.utc) - context["dag_run"].start_date
        ).total_seconds()
        print(f"Elapsed: {elapsed:.0f}s / {RUN_DURATION_SECONDS}s")
        return elapsed >= RUN_DURATION_SECONDS

    monitor_long_run_task()

dag = poll_test_dag()

if __name__ == "__main__":
    dag.test()

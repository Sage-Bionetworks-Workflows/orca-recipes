"""Hello World DAG

A minimal DAG used to verify that DAG deployments to MWAA are working. It
has no external dependencies, runs a single task that logs a message, and
is not scheduled.
"""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test"],
)
def hello_world_dag():
    @task
    def say_hello() -> None:
        print("Hello, world!")

    say_hello()


hello_world_dag()

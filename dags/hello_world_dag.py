"""Hello World DAG

A minimal DAG used to verify that DAG deployments to MWAA are working. It
has no external dependencies, runs a single task that logs a message, and
is not scheduled.

It also shows dev vs prod configuration: the DAG is tagged with the MWAA
environment it was parsed in, and the task logs a value chosen with
`by_environment`.
"""

from datetime import datetime

from airflow.sdk import dag, task

from src.environment import ENVIRONMENT, by_environment


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", ENVIRONMENT],
)
def hello_world_dag():
    @task
    def say_hello() -> None:
        print(by_environment(dev="Hello from dev!", prod="Hello from prod!"))

    say_hello()


hello_world_dag()

"""Standalone integration smoke test for passing a dataclass between tasks.

One task returns a dataclass and the next task receives it through XCom. The
second task fails with "was not found in allow list for deserialization imports"
unless `[core] allowed_deserialization_classes_regexp` allows the class. MWAA sets
it to `.*`, and so does `.env.example`.

Trigger it from the Airflow UI or with `airflow dags test`. A run with
`python dags/integration_tests/test_xcom_dataclass.py` passes even without the
setting, because the class is then `__main__.Point`, which the default allows.

This is a plain runnable script, not a pytest test. Its DAG function is
intentionally not named with a `test_` prefix (unlike the file name) so
pytest's `testpaths = tests` config doesn't try to collect it.
"""
from dataclasses import dataclass

from airflow.sdk import dag, task


@dataclass
class Point:
    x: int
    y: int


dag_config = {
    "schedule": None,
}


@dag(**dag_config)
def xcom_dataclass_test_dag():
    """Pass a `Point` from one task to the next."""

    @task
    def make() -> Point:
        """Return a Point."""
        return Point(1, 2)

    @task
    def use(point: Point) -> None:
        """Check that the Point arrived as a Point."""
        print(f"received {point!r} of type {type(point).__module__}.{type(point).__name__}")
        assert (point.x, point.y) == (1, 2)

    use(make())


dag = xcom_dataclass_test_dag()

if __name__ == "__main__":
    dag.test()

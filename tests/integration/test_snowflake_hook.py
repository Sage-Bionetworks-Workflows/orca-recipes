"""Standalone integration smoke test for SnowflakeHook.

Runs a trivial, row-limited query against Snowflake to verify that
SnowflakeHook can authenticate and execute a query end-to-end against a real
Snowflake account, without needing a real production query or waiting on a
long-running job.

This is a plain runnable script, not a pytest test: it hits a real external
service and requires real credentials for `snowflake_developer_service_conn`.
Its DAG function is intentionally not named with a `test_` prefix (unlike the
file name) so pytest's `testpaths = tests` config doesn't try to collect and
run it as a real test.

Run directly with: python3 tests/integration/test_snowflake_hook.py
"""
import _bootstrap  # noqa: F401  (sets up sys.path for dags.*/src.* imports)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import dag, task
from airflow.models.param import Param


dag_params = {
    "snowflake_developer_service_conn": Param(
        "SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
}

dag_config = {
    "schedule": None,
    "tags": ["snowflake"],
    "params": dag_params,
}

PUBLIC_QUERY =  """
SELECT
    objectdownload_event.user_id,
    objectdownload_event.file_handle_id AS FD_FILE_HANDLE_ID,
    objectdownload_event.record_date,
    objectdownload_event.project_id,
FROM
    synapse_data_warehouse.synapse_event.objectdownload_event
LIMIT 10;
"""

@dag(**dag_config)
def snowflake_hook_test_dag():
    """Smoke-test SnowflakeHook by fetching a small sample of download events.

    This DAG connects to Snowflake and pulls 10 rows from the download-event
    table, to verify that SnowflakeHook can authenticate and execute a query
    end-to-end without needing a real production query.

    DAG Parameters:

    - `snowflake_developer_service_conn`: Connection ID for the Snowflake
        developer service account.
    """

    @task()
    def get_data_from_snowflake(**context):
        """Query Snowflake for a small sample of download events.

        Arguments:
            context: Airflow context dictionary containing DAG parameters
                - snowflake_developer_service_conn: Connection ID for Snowflake

        Returns:
            None
        """
        snow_hook = SnowflakeHook(
            context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        try:
            cs.execute(PUBLIC_QUERY)
            public_df = cs.fetch_pandas_all()
            assert len(public_df) == 10
        finally:
            cs.close()
    get_data_from_snowflake()

dag = snowflake_hook_test_dag()


if __name__ == "__main__":
    dag.test()
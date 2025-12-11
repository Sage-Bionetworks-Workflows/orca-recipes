from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook

# Adjust import to your real package path inside Airflow
from snowflake_utils import get_connection, get_cursor


dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_GENIE_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_GENIE_RUNNER_SERVICE_ACCOUNT_CONN", type="string"),
}

@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    params=dag_params,
    tags=["test", "snowflake_utils"],
)
def test_snowflake_utils_dag():

    @task()
    def test_get_snowflake_connection(**context):
        """
        Validate:
          - Airflow SnowflakeHook returns a conn
          - your get_connection(conn=...) returns unchanged conn
          - cursor usage works
        """
        snowflake_conn_id = context["params"]["snowflake_conn_id"]
        snow_hook = SnowflakeHook(snowflake_conn_id)

        airflow_conn = snow_hook.get_conn()

        # Your helper: should NOT create/close anything here
        conn = get_connection(conn=airflow_conn)

        # Use cursor manually from the returned connection
        with conn.cursor() as cs:
            cs.execute("SELECT 1 AS one")
            print("get_connection -> SELECT 1:", cs.fetchone())

            cs.execute("SELECT CURRENT_VERSION(), CURRENT_REGION()")
            print("get_connection -> version/region:", cs.fetchone())

        # Do NOT close conn; Airflow owns it.
        return "ok_snowflake_connection"


    @task()
    def test_get_cursor(**context):
        """
        Validate:
          - your get_cursor(conn=...) yields a cursor
          - cursor closes correctly
          - connection is NOT closed (Airflow-owned)
        """
        snowflake_conn_id = context["params"]["snowflake_conn_id"]
        snow_hook = SnowflakeHook(snowflake_conn_id)

        airflow_conn = snow_hook.get_conn()

        # Your helper yields a cursor and will not close airflow_conn.
        with get_cursor(conn=airflow_conn) as cs:
            cs.execute("""
                SELECT
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA()
            """)
            print("get_cursor -> context:", cs.fetchone())

            cs.execute("SELECT CURRENT_TIMESTAMP()")
            print("get_cursor -> timestamp:", cs.fetchone())

        return "ok_cursor"

    
    @task()
    def test_get_genie_synapse_connection(**context):
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        # test a random file
        syn_hook.get("syn51611938")
        return "ok_genie_synapse_connection"

    # Run both (independent or you can chain)
    test_get_snowflake_connection()
    test_get_cursor()
    test_get_genie_synapse_connection()


test_snowflake_utils_dag = test_snowflake_utils_dag()

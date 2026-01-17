# Snowflake Utils

## How to Use

The functions in these scripts are meant to be imported into custom modules (outside of an Airflow DAG) that plan to use Snowflake connections locally or through Airflow (DAGs). This allows you flexibility in your code (that needs to connect to Snowflake) being able to run outside of Airflow and also inside Airflow DAGs


## Example Usage

### Connecting to Snowflake locally in your code

1. Setup your environment variables either by executing the following commands directly in your terminal. Alternatively, you can create a hidden bash script (e.g: `.env.sh`) with the below and then execute it:

    ```
    export SNOWFLAKE_USER=
    export SNOWFLAKE_ACCOUNT=
    export SNOWFLAKE_WAREHOUSE=
    export SNOWFLAKE_ROLE=
    export SNOWFLAKE_PRIVATE_KEY_FILE=
    export SNOWFLAKE_PRIVATE_KEY_FILE_PWD=
    ```

2. Add the following code in your python script(s):

    ```python
    from snowflake_utils import get_connection

    conn_obj = get_connection()

    try:
        # runs code that uses conn_obj
        ...
    finally:
        conn_obj.close()
    ```

   OR

2. If you only plan on executing code using the Snowflake connection cursor object:

    ```python
    from snowflake_utils import get_cursor

    with get_cursor() as cs:
        cs.execute(...)
    ```

### Connecting to Snowflake via Airflow's SnowflakeHook in your code

1. Add the following code to your python script(s):

    ```python
    from snowflake_utils import get_connection

    conn_obj = get_connection(conn=conn)
    try:
        # runs code that uses conn_obj
        ...
    finally:
        # if it's a local connection, close it
        if conn is None:
            conn_obj.close()
    ```

    The reason we set it up like this is because we have two execution modes with different ownership of the Snowflake connection lifecycle. When we use the Airflow's SnowflakeHook connection to connect to Snowflake, we may not want to close the connection because it could break other tasks/retries in the same Airflow context, but when it's our local connection, we want to ensure it gets closed.

    OR

    If you only plan on executing code using the Snowflake connection cursor object:

    ```python
    from snowflake_utils import get_connection


    with get_cursor(conn=conn) as cs:
        cs.execute(...)
    ```

2. Then in your DAG you would just pass in the conn object from Airflow and call your custom python module.

   In this example, the main_genie_ingestion script contains the custom snowflake function and handling stated above and is imported in from a custom module under `src/genie`

    ```
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    from genie import main_genie_ingestion   # <-- import in your script from your custom module

    dag_params = {
        "snowflake_conn_id": Param("YOUR_SNOWFLAKE_CONNECTION_STRING", type="string"),
    }

    @dag(
        params=dag_params,
        ...
    )
    def test_snowflake_utils_dag():

        @task()
        def test_get_connection(**context):
            snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
            airflow_conn = snow_hook.get_conn()

            ... # other code

            # call your ELT with the Airflow-owned conn
            main_genie_ingestion.main(
                synid=synid,
                overwrite=overwrite,
                database=database,
                conn=airflow_conn,
            )
    ```
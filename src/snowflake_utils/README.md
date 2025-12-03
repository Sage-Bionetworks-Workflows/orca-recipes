# Snowflake Utils

## How to Use

The functions in these scripts are meant to be imported into relevant DAGs to make it easier to connect to Snowflake when you want to have separate custom modules to write code that involves Snowflake outside of the DAGs themselves


## Example Usage

### Connecting to snowflake locally in your code

Setup your environment variables either by executing the following commands directly in your terminal:

```
export SNOWFLAKE_USER=
export SNOWFLAKE_ACCOUNT=
export SNOWFLAKE_WAREHOUSE=
export SNOWFLAKE_ROLE=
export SNOWFLAKE_PRIVATE_KEY_FILE=
export SNOWFLAKE_PRIVATE_KEY_FILE_PWD=
```

Alternatively, you can create a hidden bash script (e.g: `.env.sh`) with the above and executing it


Add the following code in your python script(s):

```python
from snowflake_utils import get_connection

conn_obj = get_connection()
```

OR

If you only plan on executing code using the Snowflake connection cursor object:

```python
from snowflake_utils import get_cursor

cs = get_cursor()
```

### Connecting to snowflake via Airflow's SnowflakeHook in your code

Add the following code to your python script(s)

```python
from snowflake_utils import get_connection

conn_obj = get_connection(conn=conn)
```

OR

If you only plan on executing code using the Snowflake connection cursor object:

```python
from snowflake_utils import get_connection

cs = get_cursor(conn=conn)
```

Then in your DAG you would just pass in the conn object from Airflow:
```
from genie import main_genie_elt   # <-- your script

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
}

@dag(
    params=dag_params,
    ...
)
def test_snowflake_utils_dag():

    @task()
    def test_get_connection(**context):
        snow_hook = SnowflakeHook(snowflake_conn_id)
        airflow_conn = snow_hook.get_conn()

        ... # other code

        # call your ELT with the Airflow-owned conn
        main_genie_elt.main(
            synid=synid,
            overwrite=overwrite,
            database=database,
            conn=airflow_conn,
        )
```
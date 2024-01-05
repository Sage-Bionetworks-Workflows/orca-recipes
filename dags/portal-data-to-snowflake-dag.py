from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

dag_params = {
    "snowflake_conn_id": Param(
        "SNOWFLAKE_DATA_ENGINEER_PORTAL_RAW_CONN", type="string"
    ),
    # "portal_dict": Param(
    #     {
    #         "AD": "syn11346063",
    #         "PSYCHENCODE": "syn20821313.16",
    #         "NF": "syn16858331",
    #         "ELITE": "syn51228429",
    #         "HTAN": "syn52748752",
    #         "GENIE": "syn52794526",
    #     },
    #     type="dict",
    # ),
}

portal_dict = {
    "AD": "syn11346063",
    "PSYCHENCODE": "syn20821313.16",
    "NF": "syn16858331",
    "ELITE": "syn51228429",
    "HTAN": "syn52748752",
    "GENIE": "syn52794526",
}

dag_config = {
    "schedule_interval": "0 0 * * 1",
    "start_date": datetime(2023, 2, 21),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}


@dag(**dag_config)
def portal_data_to_snowflake():
    @task
    def test_connection(**context):
        snow = SnowflakeHook(context["params"]["snowflake_conn_id"])
        # query the SAGE database in the PORTAL_RAW schema in the AD table and display the first 10 rows
        snow.run(
            "select * from AD limit 10",
        )

    test_connection()


portal_data_to_snowflake()

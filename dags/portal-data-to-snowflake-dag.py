from datetime import datetime

import pandas as pd
import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook
from snowflake.connector.pandas_tools import write_pandas

dag_params = {
    "snowflake_developer_service_conn": Param(
        "SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"
    ),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
}

portal_dict = {
        "AD": {"synapse_id": "syn11346063", "id_col": "ID"},
        "PSYCHENCODE": {"synapse_id": "syn20821313.16", "id_col": "ID"},
        "NF": {"synapse_id": "syn16858331", "id_col": "ID"},
        "ELITE": {"synapse_id": "syn51228429", "id_col": "ID"},
        "HTAN": {"synapse_id": "syn52748752", "id_col": "ENTITYID"},
        "GENIE": {"synapse_id": "syn52794526", "id_col": "ID"},
        }

dag_config = {
    "schedule_interval": "0 0 * * 1",
    "start_date": datetime(2024, 1, 10),
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
    def get_portal_data_from_synapse(**context):
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        for portal_name, info in portal_dict.items():
            synapse_id = info["synapse_id"]
            # Allow for Synapse IDs with version numbers
            ent = syn_hook.client.get(synapse_id.split(".")[0])
            if isinstance(ent, synapseclient.EntityViewSchema):
                portal = syn_hook.client.tableQuery(f"select * from {synapse_id}")
                portal_df = portal.asDataFrame()
                portal_df.reset_index(inplace=True, drop="index")
            elif isinstance(ent, synapseclient.File):
                portal_df = pd.read_csv(ent.path)
            else:
                raise TypeError(f"Unsupported entity type: {type(ent)}")
            # "grant" and "group" are reserved key words
            portal_df.rename(
                columns={"grant": "grants", "group": "groups"}, inplace=True
            )
            # Standardize column names for Snowflake tables
            portal_df.columns = [col.upper() for col in portal_df.columns]
            portal_dict[portal_name]["data"] = portal_df
        return portal_dict

    @task
    def lead_portal_data_to_snowflake(portal_data, **context):
        snow_hook = SnowflakeHook(context["params"]["snowflake_developer_service_conn"])
        # For now we will overwrite tables each time
        for portal_name, info in portal_data.items():
            write_pandas(
            snow_hook.get_conn(),
            info["data"],
            portal_name,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False
        )
            
            # TODO account for schema changes so that we can support upserts
            # Create temporary table so we can upsert
            # temp_table = f"{portal_name}_TEMP"
            # write_pandas(
            #     snow_hook.get_conn(),
            #     info["data"],
            #     temp_table,
            #     auto_create_table=True,
            #     table_type="transient",
            #     overwrite=True,
            #     quote_identifiers=False,
            # )
            # merge_sql = prepare_merge_sql(
            #     portal_name=portal_name,
            #     portal_df=info["data"],
            #     temp_table=temp_table,
            #     id_col=info["id_col"],
            # )
            # # Upsert into non-temporary tables
            # print(merge_sql)
            # snow_hook.run(merge_sql)
            # snow_hook.run(f"DROP TABLE {temp_table}")

    portal_data = get_portal_data_from_synapse()
    load = lead_portal_data_to_snowflake(portal_data=portal_data)

    portal_data >> load


portal_data_to_snowflake()

# Will use in future upsert implementation
# def prepare_merge_sql(
#     portal_name: str, temp_table: str, portal_df: pd.DataFrame, id_col: str
# ) -> str:
#     """
#     Generate a SQL merge statement for updating and inserting data into a database table.

#     Parameters:
#         portal_name (str): The name of the portal table.
#         temp_table (str): The name of the temporary table.
#         portal_df (pd.DataFrame): The DataFrame containing the portal data to be merged.
#         id_col (str): The name of the column used for matching records in the merge.

#     Returns:
#         merge_sql (str): The SQL merge statement.
#     """
#     update_set = [
#         f'"{portal_name}"."{col}" = "{temp_table}"."{col}"'
#         for col in portal_df.columns
#     ]
#     update_set_str = ",".join(update_set)
#     col_str = ",".join(f'"{col}"' for col in portal_df.columns)
#     to_insert_str = ",".join([f'"{temp_table}"."{col}"' for col in portal_df.columns])
#     merge_sql = f"""
#     MERGE INTO {portal_name} USING {temp_table}
#         ON {portal_name}.{id_col} = {temp_table}.{id_col}
#         when matched then
#             update set {update_set_str}
#         when not matched then
#         insert
#         ({col_str}) values({to_insert_str});
#     """
#     return merge_sql

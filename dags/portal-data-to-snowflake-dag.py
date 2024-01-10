from datetime import datetime

import pandas as pd
import synapseclient
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook
from snowflake.connector.pandas_tools import write_pandas

dag_params = {
    "snowflake_conn_id": Param(
        "SNOWFLAKE_DATA_ENGINEER_PORTAL_RAW_CONN", type="string"
    ),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "portal_dict": Param(
    {
        "AD": {"synapse_id": "syn11346063", "id_col": "ID"},
        "PSYCHENCODE": {"synapse_id": "syn20821313.16", "id_col": "ID"},
        "NF": {"synapse_id": "syn16858331", "id_col": "ID"},
        "ELITE": {"synapse_id": "syn51228429", "id_col": "ID"},
        "HTAN": {"synapse_id": "syn52748752", "id_col": "ENTITYID"},
        "GENIE": {"synapse_id": "syn52794526", "id_col": "ID"},
    },
    type="object",
    )
}


def prepare_merge_sql(portal_name: str, target_table: str, portal_df: pd.DataFrame, id_col: str) -> str:
    update_set = [f'"{portal_name}"."{col}" = "{target_table}"."{col}"' for col in portal_df.columns]
    update_set_str = ",".join(update_set)
    col_str = ",".join(f'"{col}"' for col in portal_df.columns)
    to_insert_str = ",".join([f'"{target_table}"."{col}"' for col in portal_df.columns])
    merge_sql = f"""
    MERGE INTO {portal_name} USING {target_table}
        ON {portal_name}.{id_col} = {target_table}.{id_col}
        when matched then
            update set {update_set_str}
        when not matched then
        insert
        ({col_str}) values({to_insert_str});
    """
    return merge_sql

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
    def get_portal_data_from_synapse(**context):
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        portal_dict = context["params"]["portal_dict"]
        for portal_name, info in portal_dict.items():
            synapse_id = info["synapse_id"]
            # Allow for version numbers
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
    def upsert_portal_data_to_snowflake(portal_data, **context):
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        for portal_name, info in portal_data.items():
            # Create temporary table so we can upsert
            target_table = f"{portal_name}_TEMP"
            print("Creating temporary table")
            write_pandas(
                snow_hook.get_conn(),
                portal_data[portal_name]["data"],
                target_table,
                auto_create_table=True,
                table_type="transient",
                overwrite=True,
                quote_identifiers=False
            )
            print("Creating merge sql")
            merge_sql = prepare_merge_sql(portal_name=portal_name, portal_df=info["data"], target_table=target_table, id_col=info["id_col"])
            # TODO account for schema changes
            # Upsert into non-temporary tables
            print(merge_sql)
            print("Upserting")
            snow_hook.run(merge_sql)
            print("Dropping temporary table")
            snow_hook.run(f"DROP TABLE {target_table}")


    portal_data = get_portal_data_from_synapse()
    upsert = upsert_portal_data_to_snowflake(portal_data=portal_data)

    portal_data >> upsert

portal_data_to_snowflake()

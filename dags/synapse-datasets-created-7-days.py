"""This script executes a query on Snowflake to retrieve data about entities created in the past few days and stores the results in a Synapse table. 
It is scheduled to run daily at 00:00."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_SYSADMIN_PORTAL_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "current_date": Param(date.today().strftime("%Y-%m-%d"), type="string"),
}

dag_config = {
    "schedule_interval": "0 0 * * *",
    "start_date": datetime(2025, 3, 1),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}

SYNAPSE_RESULTS_TABLE = "syn64919238"


@dataclass
class EntityCreated:
    """Dataclass to record 

    Attributes:
    name: The name of this entity.
    id: The unique immutable ID for this entity.
    parent_id: parent id of the entity
    node_type: Type of Node
    content_type: Content type of folder annotated,
    created_on: The date this entity was created.
    created_by: The ID of the user that created this entity.
    is_public: True to indicate if this entity is public.
    """

    name: str
    id: int
    parent_id: int
    node_type: str
    content_type: str
    created_on: str
    created_by: str
    is_public: bool


@dag(**dag_config)
def datasets_or_projects_created_7_days() -> None:
    """Execute a query on Snowflake and report the results to a Synapse table."""

    @task
    def get_datasets_projects_created_7_days(**context) -> List[EntityCreated]:
        """Execute the query on Snowflake and return the results."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_conn_id"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()
        query = f"""
        SELECT 
            name,
            id,
            parent_id,
            node_type,
            annotations:annotations:contentType:value as content_type,
            created_on,
            created_by,
            is_public,
        FROM 
            synapse_data_warehouse.synapse.node_latest
        WHERE 
            (
                node_type IN ('dataset', 'project') 
                OR 
                (content_type LIKE '%dataset%' AND content_type IS NOT NULL)
            )
        AND 
            (
                created_on >= CURRENT_TIMESTAMP - INTERVAL '7 days' 
            )
        ORDER BY created_on;
        """
        print(query)
        cs.execute(query)
        created_entity_df = cs.fetch_pandas_all()

        entity_created = []
        for _, row in created_entity_df.iterrows():
            entity_created.append(
                EntityCreated(
                    name=row["NAME"],
                    id=row["ID"],
                    node_type=row["NODE_TYPE"],
                    parent_id=row["PARENT_ID"],
                    content_type=row["CONTENT_TYPE"],
                    created_on=row["CREATED_ON"],
                    created_by=row["CREATED_BY"],
                    is_public=row["IS_PUBLIC"]
                )
            )
        return entity_created

    @task
    def push_results_to_synapse_table(entity_created: List[EntityCreated], **context) -> None:
        """Push the results to a Synapse table."""
        data = []
        today = date.today()
        for row in entity_created:
            data.append(
                [
                    row.name,
                    row.id,
                    row.node_type,
                    row.parent_id,
                    row.content_type,
                    row.created_on,
                    row.created_by,
                    row.is_public,
                    today,
                ]
            )

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=SYNAPSE_RESULTS_TABLE, values=data)
        )

    get_entity_created = get_datasets_projects_created_7_days()
    push_to_synapse_table = push_results_to_synapse_table(entity_created=get_entity_created)

    get_entity_created >> push_to_synapse_table


datasets_or_projects_created_7_days()
"""This script executes a query on Snowflake to retrieve data about entities created in the past 7 days and stores the results in a Synapse table. 
It is scheduled to run every Monday at 00:00 UTC"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook
from slack_sdk import WebClient

dag_params = {
    "snowflake_conn_id": Param("SNOWFLAKE_SYSADMIN_PORTAL_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "current_date": Param(date.today().strftime("%Y-%m-%d"), type="string"),
}

dag_config = {
    "schedule_interval": "0 0 * * 1",
    "start_date": datetime(2025, 2, 1),
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
            TO_DATE(created_on) as entity_created_date,
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
                entity_created_date >= CURRENT_TIMESTAMP - INTERVAL '7 days' 
            )
        ORDER BY entity_created_date;
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
                    created_on=row["ENTITY_CREATED_DATE"],
                    created_by=row["CREATED_BY"],
                    is_public=row["IS_PUBLIC"]
                )
            )
        return entity_created
    
    @task
    def generate_slack_message(entity_created: List[EntityCreated], **context) -> str:
        """Generate the message to be posted to the slack channel."""
        message = f":synapse: Datasets or projects created in the last 7 days \n\n"
        for index, row in enumerate(entity_created):
            if row.content_type:
                type = row.content_type.strip("[] \n").strip('"')
            else:
                type = row.node_type
            message += f"{index+1}. <https://www.synapse.org/#!Synapse:syn{row.id}|*{row.name}*> (Type: {type}, Created on: {row.created_on}, Created by: <https://www.synapse.org/Profile:{row.created_by}/profile|this user>, Public: {row.is_public})\n\n"
        return message

    @task
    def post_slack_messages(message:str) -> bool:
        """Post the top downloads to the slack channel."""
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        result = client.chat_postMessage(channel="hotdrop_test", text=message)
        print(f"Result of posting to slack: [{result}]")
        return result is not None

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

    entity_created = get_datasets_projects_created_7_days()
    # push_to_synapse_table = push_results_to_synapse_table(entity_created=entity_created)
    slack_message = generate_slack_message(entity_created=entity_created)
    post_to_slack = post_slack_messages(message=slack_message)

    entity_created >> slack_message
    slack_message >> post_to_slack
    # entity_created >> push_to_synapse_table


datasets_or_projects_created_7_days()
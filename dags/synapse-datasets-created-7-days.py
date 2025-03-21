"""This script executes a query on Snowflake to retrieve data about entities created in the past 7 days and stores the results in a Synapse table.
It is scheduled to run every Monday at 00:00 UTC.

A Note on the `backfill` functionality:
In addition to the fact that this DAG pulls data from the day prior to the provided date, there is an extra wrinkle when using the `backfill` functionality.
In the Synapse table UI, data is displayed at the local datetime of the user, but the Snowflake query is performed at UTC time. So, if we take into account both of these time differences
for someone living in North America, you will need to provide a `backfill_date` 2 days after the date that the data is missing in the Synapse table. For example,
if data is missing for "2025-01-03", `backfill_date` will need to be set to "2025-01-05". Additionally, be sure to set `backfill` to `true` or the DAG will run normally and post
the results to Slack.

DAG Parameters:
- `snowflake_developer_service_conn`: A JSON-formatted string containing the connection details required to authenticate and connect to Snowflake.
- `synapse_conn_id`: The connection ID for the Synapse connection.
- 'current_date_time': The current date time in UTC timezone
- `backfill`: Whether to backfill the data. Defaults to `False`.
- `backfill_date_time`: The date time to backfill the data from. in UTC time zone. Will be ignored if `backfill` is `False`.
"""
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import List

import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from orca.services.synapse import SynapseHook
from slack_sdk import WebClient

dag_params = {
    "snowflake_developer_service_conn": Param("SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "current_date_time": Param(
        datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S"), type="string"
    ),
    "backfill": Param(False, type="boolean"),
    # backfill_date_time string format: YYYY-MM-DD HH:MM:SS
    "backfill_date_time": Param("1900-01-01 00:00:00", type="string"),
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

SYNAPSE_RESULTS_TABLE = "syn64951484"


@dataclass
class EntityCreated:
    """Dataclass to record

    Attributes:
        name: The name of this entity.
        id: The unique immutable ID for this entity.
        project_id: project id of the entity
        node_type: Type of Node
        content_type: Content type of folder annotated,
        created_on: The date this entity was created.
        user_name_full_name: Full name of the user that created this entity. If first name or last name is null, use user name
        created_by: The ID of the user that created this entity.
        is_public: True to indicate if this entity is public.
    """

    name: str
    id: int
    project_id: int
    node_type: str
    content_type: str
    created_on: str
    user_name_full_name: str
    created_by: str
    is_public: bool


@dag(**dag_config)
def datasets_or_projects_created_7_days() -> None:
    """Execute a query on Snowflake and report the results to a Synapse table."""

    @task.branch()
    def check_backfill(**context) -> str:
        """Check if the backfill is enabled. When it is, do not post to Slack."""
        if context["params"]["backfill"]:
            return "stop_dag"
        return "generate_slack_message"

    @task()
    def stop_dag() -> None:
        """Stop the DAG."""
        pass

    @task
    def get_datasets_projects_created_7_days(**context) -> List[EntityCreated]:
        """Execute the query on Snowflake and return the results."""
        snow_hook = SnowflakeHook(context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()

        # set backfill_date to None if backfill is False
        backfill_date_time = context["params"]["backfill_date_time"]
        query_date = (
            None if not context["params"]["backfill"] else f"{backfill_date_time}"
        )

        query = f"""
        SELECT 
            name,
            n.id,
            project_id,
            node_type,
            ARRAY_TO_STRING(annotations:annotations:contentType:value, ', ') as content_type,
            TO_DATE(n.created_on) as entity_created_date,
            created_by,
            CASE WHEN last_name IS NULL OR first_name is NULL THEN user_name ELSE CONCAT(first_name,' ',last_name)END as user_name_full_name,
            is_public,
        FROM 
            synapse_data_warehouse.synapse.node_latest as n
        LEFT JOIN 
            synapse_data_warehouse.synapse.userprofile_latest as u
            ON 
            n.created_by = u.id
        WHERE 
            (
                node_type IN ('dataset', 'project') 
                OR 
                (content_type = 'dataset')
            )
        AND 
            (
                entity_created_date >= DATEADD(DAY, -7, '{query_date or context["params"]["current_date_time"]}')
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
                    project_id=row["PROJECT_ID"],
                    content_type=row["CONTENT_TYPE"],
                    created_on=row["ENTITY_CREATED_DATE"],
                    user_name_full_name=row["USER_NAME_FULL_NAME"],
                    created_by=row["CREATED_BY"],
                    is_public=row["IS_PUBLIC"],
                )
            )
        return entity_created

    @task
    def generate_slack_message(entity_created: List[EntityCreated], **context) -> str:
        """Generate the message to be posted to the slack channel."""
        message = ":synapse: Datasets or projects created in the last 7 days \n\n"
        for index, row in enumerate(entity_created):
            if row.content_type:
                data_type = row.content_type
            else:
                data_type = row.node_type
            message += f"{index+1}. <https://www.synapse.org/#!Synapse:syn{row.id}|*{row.name}*> (Type: {data_type}, Created on: {row.created_on}, Created by: <https://www.synapse.org/Profile:{row.created_by}/profile|{row.user_name_full_name}>, Public: {row.is_public})\n\n"
        return message

    @task
    def post_slack_messages(message: str) -> bool:
        """Post the top downloads to the slack channel."""
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        result = client.chat_postMessage(channel="hotdrops", text=message)
        print(f"Result of posting to slack: [{result}]")
        return result is not None

    @task
    def push_results_to_synapse_table(
        entity_created: List[EntityCreated], **context
    ) -> None:
        """Push the results to a Synapse table."""
        data = []
        # convert context["params"]["backfill_date_time"] to date in same format as date.today()
        today = (
            date.today()
            if not context["params"]["backfill"]
            else datetime.strptime(
                context["params"]["backfill_date_time"], "%Y-%m-%d  %H:%M:%S"
            ).date()
        )
        for row in entity_created:
            data.append(
                [
                    row.name,
                    row.id,
                    row.project_id,
                    row.node_type,
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
    check = check_backfill()
    stop = stop_dag()
    push_to_synapse_table = push_results_to_synapse_table(entity_created=entity_created)
    slack_message = generate_slack_message(entity_created=entity_created)
    post_to_slack = post_slack_messages(message=slack_message)

    entity_created >> check >> [stop, slack_message]
    slack_message >> post_to_slack
    entity_created >> push_to_synapse_table


datasets_or_projects_created_7_days()

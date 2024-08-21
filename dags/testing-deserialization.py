"""This script is used to execute a query on Snowflake and report the results to a
slack channel and Synapse table. 
This retrieves the top X publicly downloaded Synapse projects for slack.
This retrieves all publicly downloaded Synapse projects for the Synapse table.
See ORCA-301 for more context."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
    # hours_time_delta is the number of hours to subtract from the current date to get the date for the query
    "hours_time_delta": Param("24", type="string"),
    "backfill": Param(False, type="boolean"),
    # backfill_date string format: YYYY-MM-DD
    "backfill_date": Param("1900-01-01", type="string"),
}

dag_config = {
    "schedule_interval": "0 18 * * *",
    "start_date": datetime(2024, 2, 20),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["snowflake"],
    "params": dag_params,
}

SIZE_ROUNDING = 3
BYTE_STRING = "GiB"
# 30 is the power of 2 for GiB, 40 is the power of 2 for TiB
POWER_OF_TWO = 30

SYNAPSE_RESULTS_TABLE = "syn53696951"


@dataclass
class DownloadMetric:
    """Dataclass to hold the download metrics from Synapse.

    Attributes:
        name: The name of the project
        project: The ID of the project
        downloads_per_project: The number of downloads per project
        number_of_unique_users_downloaded: The number of unique users who downloaded
        data_download_size: The size of the data downloaded

    """

    name: str
    project: str
    downloads_per_project: int
    number_of_unique_users_downloaded: int
    data_download_size: float


@dataclass
class SynTestingData:
    """Dataclass to hold the download metrics from Synapse.

    Attributes:
        name: The name of the project
        project: The ID of the project
        downloads_per_project: The number of downloads per project
        number_of_unique_users_downloaded: The number of unique users who downloaded
        data_download_size: The size of the data downloaded

    """

    name: str
    project: str
    downloads_per_project: int
    number_of_unique_users_downloaded: int
    data_download_size: float


@dag(**dag_config)
def top_public_synapse_projects_from_snowflake_testing() -> None:
    """Execute a query on Snowflake and report the results to a slack channel and
    synapse table."""

    @task
    def get_top_downloads_from_snowflake(**context) -> List[DownloadMetric]:
        """Execute the query on Snowflake and return the results."""
        metrics = []
        metrics.append(
            DownloadMetric(
                name="test",
                project="test1",
                downloads_per_project="test3",
                number_of_unique_users_downloaded="test4",
                data_download_size=1,
            )
        )
        print(metrics)
        return metrics

    @task
    def get_top_downloads_from_snowflake_Test(**context) -> List[SynTestingData]:
        """Execute the query on Snowflake and return the results."""
        metrics = []
        metrics.append(
            SynTestingData(
                name="test",
                project="test1",
                downloads_per_project="test3",
                number_of_unique_users_downloaded="test4",
                data_download_size=1,
            )
        )
        print(metrics)
        return metrics

    @task.branch()
    def check_backfill(**context) -> str:
        """Check if the backfill is enabled. When it is, do not post to Slack."""
        if context["params"]["backfill"]:
            return "stop_dag"
        return "generate_top_downloads_message"

    @task()
    def stop_dag() -> None:
        """Stop the DAG."""
        pass

    @task
    def generate_top_downloads_message(metrics, **context) -> str:
        """Generate the message to be posted to the slack channel."""
        message = "this is a test message"
        return message

    @task
    def post_top_downloads_to_slack(message: str) -> str:
        """Post the top downloads to the slack channel."""
        print(f"Not a real slack message: {message}")
        return message

    @task
    def push_results_to_synapse_table(metrics: List[DownloadMetric], **context) -> None:
        """Push the results to a Synapse table."""
        print(f"Not a real Synapse table push: {metrics}")

    top_downloads = get_top_downloads_from_snowflake()
    top_downloads_1 = get_top_downloads_from_snowflake_Test()
    check = check_backfill()
    stop = stop_dag()
    slack_message = generate_top_downloads_message(metrics=top_downloads)
    post_to_slack = post_top_downloads_to_slack(message=slack_message)
    push_to_synapse_table = push_results_to_synapse_table(
        metrics=top_downloads)

    top_downloads >> check >> [stop, slack_message]
    top_downloads_1 >> slack_message
    slack_message >> post_to_slack
    top_downloads >> push_to_synapse_table


top_public_synapse_projects_from_snowflake_testing()

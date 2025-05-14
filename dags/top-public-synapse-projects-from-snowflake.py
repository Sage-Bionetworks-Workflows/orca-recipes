"""This DAG executes a query on Snowflake retrieving the top X most downloaded Synapse
projects from the day prior to the provided date (defaults to today's date) and report
the results to a Slack channel and Synapse table.
The query includes both public projects and approved private projects that are
specified in file views through the fileview_groups parameter. Projects from these file
views are grouped by the specified group name in reports.
See ORCA-301 for more context.

Only public projects are eligible for the Synapse table storage. Both public projects
and approved private projects are eligible for Slack reporting.
"""

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
import json

dag_params = {
    "snowflake_developer_service_conn": Param(
        "SNOWFLAKE_DEVELOPER_SERVICE_RAW_CONN", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    # hours_time_delta is the number of hours to subtract from the current date to get
    # the date for the query
    "hours_time_delta": Param("24", type="string"),
    "backfill": Param(False, type="boolean"),
    # backfill_date string format: YYYY-MM-DD
    "backfill_date": Param("1900-01-01", type="string"),
    # fileview_groups is a JSON string containing an array of objects with file_view_id
    # and group_name properties
    # Example: '[{"file_view_id": "123456", "group_name": "Group A"},
    #           {"file_view_id": "789012", "group_name": "Group B"}]'
    "fileview_groups": Param(
        '[{"file_view_id": "20446927", "group_name": "HTAN1"}]', type="string"),
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

# ID of the Synapse table where aggregated results will be stored for public viewing
SYNAPSE_RESULTS_TABLE = "syn53696951"
# ID of the Synapse homepage project, excluded from download stats
SYNAPSE_HOMEPAGE_PROJECT_ID = 23593546


@dataclass
class DownloadMetric:
    """Dataclass to hold the download metrics from Synapse.

    Attributes:
        name: The name of the project or group
        project: The ID of the project or fileview
        projects_with_downloads: The number of unique projects that had downloads
        total_projects: The total number of projects in the group
        downloads_per_project: The number of downloads per project
        number_of_unique_users_downloaded: The number of unique users who downloaded
        data_download_size: The size of the data downloaded in bytes
    """

    name: str
    project: str
    projects_with_downloads: int = 0
    total_projects: int = 0
    downloads_per_project: int = 0
    number_of_unique_users_downloaded: int = 0
    data_download_size: float = 0


@dag(**dag_config)
def top_public_synapse_projects_from_snowflake() -> None:
    """Execute a query on Snowflake retrieving the top most downloaded Synapse projects.

    This DAG retrieves download statistics for Synapse projects from Snowflake
    and reports the results to a Slack channel and Synapse table. It supports both
    public projects and approved private projects specified in fileviews.


    DAG Parameters:

    - `snowflake_developer_service_conn`: A JSON-formatted string containing the 
        connection details required to authenticate and connect to Snowflake.
    - `synapse_conn_id`: The connection ID for the Synapse connection.
    - `hours_time_delta`: The number of hours to subtract from the current date to 
        get the date for the query. Defaults to `24`.
    - `backfill`: Whether to backfill the data. Defaults to `False`. When set to True, 
        the DAG will not post to Slack and will only update the Synapse table.
    - `backfill_date`: The date to backfill the data from, in YYYY-MM-DD format. 
        Will be ignored if `backfill` is `False`.
        Note on backfill timing: Due to time zone differences between Synapse table UI 
        (local time) and Snowflake queries (UTC), users in North America may need to set 
        the backfill_date to 2 days after the missing date in the Synapse table.
        For example, if data is missing for "2025-01-03" in the Synapse table, set 
        `backfill_date` to "2025-01-05".
    - `fileview_groups`: A JSON string containing an array of objects with 
        `file_view_id` and `group_name` properties. 
        Projects from these fileviews will be grouped together under their group name 
        in reports and excluded from Synapse table storage.
        The implementation queries the SCOPE_IDS column of the fileview in the Snowflake 
        warehouse to find all projects that are part of the fileview. Each fileview's 
        project IDs are then grouped under the provided group_name in the reports.
        Note: The `file_view_id` should be provided WITHOUT the 'syn' prefix as it's 
        used directly in Snowflake queries.
        Example: `[{"file_view_id": "123456", "group_name": "Group A"}, 
                    {"file_view_id": "789012", "group_name": "Group B"}]`
    """

    @task
    def get_public_downloads_from_snowflake(**context) -> List[DownloadMetric]:
        """Execute a query on Snowflake and return download metrics for public projects.

        This function executes a query to get download statistics for all public Synapse 
        projects for the specified time period.

        Arguments:
            context: Airflow context dictionary containing DAG parameters
                - snowflake_developer_service_conn: Connection ID for Snowflake
                - backfill_date: Date string for backfill in YYYY-MM-DD format
                - hours_time_delta: Hours to subtract from current/backfill date
                - backfill: Boolean indicating whether this is a backfill run

        Returns:
            List[DownloadMetric]: A list of download metrics for public projects
        """
        snow_hook = SnowflakeHook(
            context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()

        # set backfill_date to None if backfill is False
        backfill_date = context["params"]["backfill_date"]
        hours_time_delta = context["params"]["hours_time_delta"]

        metrics = []

        is_backfill = context["params"]["backfill"]
        hours_delta = -int(hours_time_delta)

        # Different query templates for backfill vs current date
        # For backfill we parameterize the date, for current we use CURRENT_DATE SQL keyword
        date_clause = "DATEADD(HOUR, %(hours_delta)s, %(backfill_date)s)" if is_backfill else "DATEADD(HOUR, %(hours_delta)s, CURRENT_DATE)"

        # Get download stats for public projects - with parameterization
        public_query = f"""
            WITH PUBLIC_PROJECTS AS (
                SELECT
                    node_latest.project_id,
                    node_latest.name
                FROM
                    synapse_data_warehouse.synapse.node_latest
                WHERE
                    node_latest.is_public AND
                    node_latest.node_type = 'project' AND
                    node_latest.project_id != %(homepage_id)s
            ),
            DEDUP_FILEHANDLE AS (
                SELECT DISTINCT
                    PUBLIC_PROJECTS.name,
                    filedownload.user_id,
                    filedownload.file_handle_id AS FD_FILE_HANDLE_ID,
                    filedownload.record_date,
                    filedownload.project_id,
                    file_latest.content_size
                FROM
                    synapse_data_warehouse.synapse.filedownload
                INNER JOIN
                    PUBLIC_PROJECTS
                ON
                    filedownload.project_id = PUBLIC_PROJECTS.project_id
                INNER JOIN
                    synapse_data_warehouse.synapse.file_latest
                ON
                    filedownload.file_handle_id = file_latest.id
                WHERE
                    filedownload.record_date = {date_clause}
            ),

            DOWNLOAD_STAT AS (
                SELECT
                    name,
                    project_id,
                    count(record_date) AS DOWNLOADS_PER_PROJECT,
                    count(DISTINCT user_id) AS NUMBER_OF_UNIQUE_USERS_DOWNLOADED,
                    count(DISTINCT FD_FILE_HANDLE_ID) AS NUMBER_OF_UNIQUE_FILES_DOWNLOADED,
                    sum(content_size) as data_download_size
                FROM
                    DEDUP_FILEHANDLE
                GROUP BY
                    project_id, name
            )
            SELECT
                'syn' || cast(DOWNLOAD_STAT.project_id as varchar) as project,
                DOWNLOAD_STAT.name,
                DOWNLOAD_STAT.DOWNLOADS_PER_PROJECT,
                DOWNLOAD_STAT.data_download_size,
                DOWNLOAD_STAT.NUMBER_OF_UNIQUE_USERS_DOWNLOADED
            FROM
                DOWNLOAD_STAT
            ORDER BY
                NUMBER_OF_UNIQUE_USERS_DOWNLOADED DESC NULLS LAST
        """

        try:
            # Execute query with parameters - handling CURRENT_DATE specially in the query string
            query_params = {
                'hours_delta': hours_delta,
                'backfill_date': backfill_date,
                'homepage_id': SYNAPSE_HOMEPAGE_PROJECT_ID
            }
            cs.execute(public_query, query_params)
            public_df = cs.fetch_pandas_all()

            # Create metrics for public projects
            for _, row in public_df.iterrows():
                metrics.append(
                    DownloadMetric(
                        name=row["NAME"],
                        project=row["PROJECT"],
                        downloads_per_project=row["DOWNLOADS_PER_PROJECT"],
                        number_of_unique_users_downloaded=row[
                            "NUMBER_OF_UNIQUE_USERS_DOWNLOADED"
                        ],
                        data_download_size=row["DATA_DOWNLOAD_SIZE"] or 0,
                    )
                )
        finally:
            cs.close()

        return metrics

    @task
    def get_fileview_downloads_from_snowflake(**context) -> List[DownloadMetric]:
        """Execute a query on Snowflake and return download metrics for fileview groups.

        This function executes a single query that:
        1. Parses the fileview_groups parameter to get file view IDs and group names
        2. Uses the file view scopes to identify projects belonging to each group
        3. Gets download stats for those projects, grouped by their fileview groups
        4. Allows projects to appear in multiple groups if they are in multiple fileviews

        Arguments:
            context: Airflow context dictionary containing DAG parameters
                - snowflake_developer_service_conn: Connection ID for Snowflake
                - fileview_groups: JSON string of fileview ID and group name mappings
                - backfill_date: Date string for backfill in YYYY-MM-DD format
                - hours_time_delta: Hours to subtract from current/backfill date
                - backfill: Boolean indicating whether this is a backfill run

        Returns:
            List[DownloadMetric]: A list of download metrics for fileview groups
        """
        groups = json.loads(context["params"]["fileview_groups"])

        if not groups:
            return []

        snow_hook = SnowflakeHook(
            context["params"]["snowflake_developer_service_conn"])
        ctx = snow_hook.get_conn()
        cs = ctx.cursor()

        backfill_date = context["params"]["backfill_date"]
        hours_time_delta = context["params"]["hours_time_delta"]

        # Prepare query parameters
        is_backfill = context["params"]["backfill"]
        hours_delta = -int(hours_time_delta)  # Negative for lookback

        # Create date clause based on whether this is a backfill or not
        date_clause = "DATEADD(HOUR, %(hours_delta)s, %(backfill_date)s)" if is_backfill else "DATEADD(HOUR, %(hours_delta)s, CURRENT_DATE)"

        file_view_mappings = []
        for group in groups:
            if "file_view_id" not in group or "group_name" not in group:
                continue

            file_view_id = group["file_view_id"]
            group_name = group["group_name"]

            file_view_mappings.append((file_view_id, group_name))

        if not file_view_mappings:
            return []

        values_clause = ", ".join([f"(%(file_view_id_{i})s, %(group_name_{i})s)"
                                  for i in range(len(file_view_mappings))])

        fileview_query = f"""
            WITH FILEVIEW_MAPPINGS AS (
                -- Create a temporary table of file view IDs to group names
                SELECT 
                    column1::NUMBER as file_view_id,
                    column2::VARCHAR as group_name
                FROM VALUES
                    {values_clause}
            ),
            
            FILEVIEW_PROJECTS AS (
                -- Get all projects that are part of each file view
                SELECT
                    FILEVIEW_MAPPINGS.group_name,
                    FILEVIEW_MAPPINGS.file_view_id,
                    value::NUMBER as project_id
                FROM 
                    synapse_data_warehouse.synapse.node_latest,
                    LATERAL FLATTEN(input => SCOPE_IDS) as flattened,
                    FILEVIEW_MAPPINGS
                WHERE 
                    node_latest.id = FILEVIEW_MAPPINGS.file_view_id
            ),
            
            PROJECT_INFO AS (
                -- Get project information for all projects in file views
                SELECT
                    node_latest.project_id,
                    node_latest.name as project_name,
                    FILEVIEW_PROJECTS.group_name,
                    FILEVIEW_PROJECTS.file_view_id
                FROM
                    synapse_data_warehouse.synapse.node_latest
                INNER JOIN
                    FILEVIEW_PROJECTS
                ON 
                    node_latest.project_id = FILEVIEW_PROJECTS.project_id
                WHERE
                    node_latest.node_type = 'project'
            ),
            
            PROJECT_COUNT AS (
                -- Count total number of projects in each group
                SELECT
                    group_name,
                    file_view_id,
                    COUNT(DISTINCT project_id) as TOTAL_PROJECTS
                FROM
                    PROJECT_INFO
                GROUP BY
                    group_name, file_view_id
            ),
            
            DEDUP_FILEHANDLE AS (
                -- Get download information for files in projects from file views
                SELECT DISTINCT
                    PROJECT_INFO.group_name,
                    PROJECT_INFO.file_view_id,
                    filedownload.user_id,
                    filedownload.file_handle_id AS FD_FILE_HANDLE_ID,
                    filedownload.record_date,
                    filedownload.project_id,
                    file_latest.content_size
                FROM
                    synapse_data_warehouse.synapse.filedownload
                INNER JOIN PROJECT_INFO
                ON filedownload.project_id = PROJECT_INFO.project_id
                INNER JOIN synapse_data_warehouse.synapse.file_latest
                ON filedownload.file_handle_id = file_latest.id
                WHERE
                    filedownload.record_date = {date_clause}
            ),
            
            DOWNLOAD_STAT AS (
                -- Aggregate download statistics by group
                SELECT
                    group_name as name,
                    file_view_id,
                    count(DISTINCT project_id) as PROJECTS_WITH_DOWNLOADS,
                    count(record_date) AS DOWNLOADS_PER_PROJECT,
                    count(DISTINCT user_id) AS NUMBER_OF_UNIQUE_USERS_DOWNLOADED,
                    count(DISTINCT FD_FILE_HANDLE_ID) AS NUMBER_OF_UNIQUE_FILES_DOWNLOADED,
                    sum(content_size) as data_download_size
                FROM DEDUP_FILEHANDLE
                GROUP BY group_name, file_view_id
            )
            
            -- Return the final results
            SELECT
                'syn' || DOWNLOAD_STAT.file_view_id as project,
                name,
                PROJECTS_WITH_DOWNLOADS,
                PROJECT_COUNT.TOTAL_PROJECTS,
                DOWNLOADS_PER_PROJECT,
                data_download_size,
                NUMBER_OF_UNIQUE_USERS_DOWNLOADED
            FROM DOWNLOAD_STAT
            LEFT JOIN PROJECT_COUNT
            ON DOWNLOAD_STAT.file_view_id = PROJECT_COUNT.file_view_id
               AND DOWNLOAD_STAT.name = PROJECT_COUNT.group_name
        """

        metrics = []

        # Create a parameters dictionary for bind variables
        query_params = {}

        for i, (file_view_id, group_name) in enumerate(file_view_mappings):
            query_params[f"file_view_id_{i}"] = file_view_id
            query_params[f"group_name_{i}"] = group_name

        query_params["hours_delta"] = hours_delta
        query_params["backfill_date"] = backfill_date

        try:
            # Execute the query with parameters
            # Date references are handled through string formatting in the SQL query
            cs.execute(fileview_query, query_params)
            result_df = cs.fetch_pandas_all()

            # Create metrics from the query results
            for _, row in result_df.iterrows():
                metrics.append(
                    DownloadMetric(
                        name=row["NAME"],
                        project=row["PROJECT"],
                        projects_with_downloads=row.get(
                            "PROJECTS_WITH_DOWNLOADS", 0),
                        total_projects=row.get("TOTAL_PROJECTS", 0),
                        downloads_per_project=row["DOWNLOADS_PER_PROJECT"],
                        number_of_unique_users_downloaded=row[
                            "NUMBER_OF_UNIQUE_USERS_DOWNLOADED"
                        ],
                        data_download_size=row["DATA_DOWNLOAD_SIZE"] or 0,
                    )
                )
        except Exception as e:
            print(f"Error executing fileview download query: {e}")
        finally:
            cs.close()

        return metrics

    @task
    def combine_download_metrics(
        public_metrics: List[DownloadMetric],
        fileview_metrics: List[DownloadMetric]
    ) -> List[DownloadMetric]:
        """Combine the metrics from public projects and fileview groups.

        This function takes the results from both queries and combines them into a single list,
        sorted by the number of unique users.

        Arguments:
            public_metrics: List of download metrics from public projects
            fileview_metrics: List of download metrics from fileview groups

        Returns:
            List[DownloadMetric]: Combined and sorted list of download metrics
        """
        combined_metrics = public_metrics + fileview_metrics

        combined_metrics.sort(key=lambda x: x.number_of_unique_users_downloaded,
                              reverse=True)
        return combined_metrics

    @task.branch()
    def check_backfill(**context) -> str:
        """Check if the backfill is enabled.

        When backfill=True, the DAG will:
        1. Skip posting to Slack (by returning "stop_dag")
        2. Only update the Synapse table with historical data
        3. Use the date specified in backfill_date parameter

        This allows for filling gaps in the Synapse table without affecting Slack reporting.

        Arguments:
            context: Airflow context dictionary containing DAG parameters
                - backfill: Boolean indicating whether this is a backfill run

        Returns:
            str: Task ID to execute next ("stop_dag" or "generate_top_downloads_message")
        """
        if context["params"]["backfill"]:
            return "stop_dag"
        return "generate_top_downloads_message"

    @task()
    def stop_dag() -> None:
        """Stop the DAG execution flow.

        This is a no-op function used as a branch target when backfill is enabled,
        allowing the DAG to skip Slack notification.

        Arguments:
            None

        Returns:
            None
        """
        pass

    @task
    def generate_top_downloads_message(metrics: List[DownloadMetric], **context) -> str:
        """Generate the message to be posted to the Slack channel.

        Creates a formatted message with the top downloaded Synapse projects,
        including download counts, unique users, and data size information.

        Arguments:
            metrics: List of download metrics for projects, sorted by popularity
            context: Airflow context dictionary containing DAG parameters
                - hours_time_delta: Hours to subtract from current date

        Returns:
            str: Formatted message for Slack notification
        """
        hours_delta = int(context['params']['hours_time_delta'])
        date_str = 'Yesterday' if hours_delta <= 24 else (
            date.today() - timedelta(hours=hours_delta)).isoformat()

        message = f":synapse: Top Downloaded Public Synapse Projects {date_str}!\n\n"
        for index, row in enumerate(metrics[:10]):
            if row.data_download_size:
                size_string = (
                    f"{(row.data_download_size / 2 ** POWER_OF_TWO):.{SIZE_ROUNDING}f} "
                    f"{BYTE_STRING}"
                )
            else:
                size_string = f"< {0:.{SIZE_ROUNDING}f}5 {BYTE_STRING}"

            message += (
                f"{index+1}. <https://www.synapse.org/#!Synapse:{row.project}|{row.name}> - "
                f"{row.downloads_per_project} downloads, {row.number_of_unique_users_downloaded} "
                f"unique users, {size_string} egressed"
            )

            if hasattr(row, 'total_projects') and row.total_projects > 0:
                message += (
                    f" ({row.projects_with_downloads}/{row.total_projects} projects with downloads)")

            message += "\n\n"
        message += "One download is a user downloading an entity (File, Table, Views, etc) once\n"
        return message

    @task
    def post_top_downloads_to_slack(message: str) -> bool:
        """Post the top downloads to the Slack channel.

        Arguments:
            message: Formatted message containing top download information

        Returns:
            bool: True if message was successfully posted, False otherwise
        """

        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        result = client.chat_postMessage(channel="topcharts", text=message)
        print(f"Result of posting to slack: [{result}]")
        return result is not None

    @task
    def push_results_to_synapse_table(
        metrics: List[DownloadMetric],
        **context
    ) -> None:
        """Push the results to a Synapse table. Exclude projects from fileview groups.

        When running in backfill mode (backfill=True and a valid backfill_date), this task:
        1. Uses the backfill_date instead of the current date to determine the record date
        2. Adjusts the date by the hours_time_delta to get the correct historical data
        3. Still filters out projects from fileview groups before storing data

        Note on timezone differences: The Synapse table UI displays dates in local time,
        while Snowflake queries run in UTC. Users in North America may need to set 
        backfill_date 2 days after the missing date in the Synapse table.

        Arguments:
            metrics: List of download metrics for projects
            context: Airflow context dictionary containing DAG parameters
                - backfill: Boolean indicating whether this is a backfill run
                - backfill_date: Date string for backfill in YYYY-MM-DD format
                - hours_time_delta: Hours to subtract from current/backfill date
                - fileview_groups: JSON string of fileview ID and group name mappings
                - synapse_conn_id: Connection ID for Synapse

        Returns:
            None
        """
        data = []
        # convert backfill_date to date in same format as date.today()
        today = (
            date.today()
            if not context["params"]["backfill"]
            else datetime.strptime(
                context["params"]["backfill_date"], "%Y-%m-%d"
            ).date()
        )
        yesterday = today - timedelta(
            hours=int(context["params"]["hours_time_delta"])
        )

        # Filter out fileview group projects - they have the fileview's syn ID, not a project syn ID
        filtered_metrics = [
            metric for metric in metrics
            if not any(f.startswith("syn") and metric.project == f
                       for f in [f"syn{group['file_view_id']}"
                                 for group in json.loads(context["params"]["fileview_groups"])])
        ]

        for metric in filtered_metrics:
            data.append(
                [
                    metric.project,
                    metric.downloads_per_project,
                    yesterday,
                    metric.number_of_unique_users_downloaded,
                    metric.data_download_size,
                ]
            )

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        syn_hook.client.store(
            synapseclient.Table(schema=SYNAPSE_RESULTS_TABLE, values=data)
        )

    public_downloads = get_public_downloads_from_snowflake()
    fileview_downloads = get_fileview_downloads_from_snowflake()
    top_downloads = combine_download_metrics(
        public_metrics=public_downloads, fileview_metrics=fileview_downloads)
    check = check_backfill()
    stop = stop_dag()
    slack_message = generate_top_downloads_message(metrics=top_downloads)
    post_to_slack = post_top_downloads_to_slack(message=slack_message)
    push_to_synapse_table = push_results_to_synapse_table(
        metrics=top_downloads)

    # Set up task dependencies
    public_downloads >> top_downloads
    fileview_downloads >> top_downloads
    top_downloads >> check >> [stop, slack_message]
    slack_message >> post_to_slack
    top_downloads >> push_to_synapse_table


top_public_synapse_projects_from_snowflake()

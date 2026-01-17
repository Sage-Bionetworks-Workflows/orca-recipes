"""This script ingests the data from Jira across Sage and ingests it into snowflake"""

import os
import datetime
import pytz

from dotenv import dotenv_values
from jira import JIRA
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from jira_utils import get_issues

JIRA_SPACES = [
    "GENIE",
    "AG",
    "ADEL"
]


def main():
    """Invoke jira metrics"""
    username = os.environ["JIRA_USERNAME"]
    api_token = os.environ["JIRA_API_TOKEN"]
    jira_client = JIRA(
        server="https://sagebionetworks.jira.com/", basic_auth=(username, api_token)
    )
    # jira_project_str = "','".join(TECH_JIRA_PROJECTS)
    for program in JIRA_SPACES:
        jql = f"project = '{program}'"
        print(jql)
        program_issues = get_issues(
            jira_client=jira_client, jql=jql
        )

        date_columns = ["created_on", "start_date", "resolution_date"]
        for date_column in date_columns:
            program_issues[date_column] = pd.to_datetime(
                program_issues[date_column], utc=True, errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            # HACK: Error: Expression type does not match column data type, expecting DATE but got NUMBER(38,0) for column CREATED_ON
            # This error occurs but the `write_pandas` function seems to convert a column with empty values to a number type columns
            if program_issues[date_column].isnull().all():
                del program_issues[date_column]
            # tried these below, but they don't work
            # all_sprint_info[date_column].fillna(pd.NaT, inplace=True)
            # all_sprint_info[date_column] = all_sprint_info[date_column].astype('datetime64[ns]')
        program_issues["export_date"] = datetime.datetime.now(
            pytz.UTC
        ).strftime("%Y-%m-%d")
        program_issues.to_csv(f"{program}_tickets.csv", index=False)

        config = dotenv_values(".env")

        ctx = snowflake.connector.connect(
            user=config["user"],
            # password=config['password'],
            account=config["snowflake_account"],
            private_key_file=config["private_key_file"],
            private_key_file_pwd=config["private_key_file_pwd"],
            database="DATA_ANALYTICS",
            schema="JIRA",
            role="DATA_ANALYTICS",
            warehouse="compute_xsmall",
        )
        write_pandas(
            ctx,
            df=program_issues,
            table_name=program,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )


if __name__ == "__main__":
    main()

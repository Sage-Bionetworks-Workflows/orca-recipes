"""This script ingests the data from Jira across Sage and ingests it into snowflake
"""
import os
import datetime
from unittest import result
import pytz
from typing import Union

from dotenv import dotenv_values
from jira import JIRA
import jira
import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def _extract_issue_details(jira_client: JIRA, issue: Union[str, jira.resources.Issue]) -> dict:
    """
    Extracts the relevant information from a Jira issue object.

    This function takes a Jira issue object and extracts the relevant
    information from it. The information is then returned as a dictionary.

    Args:
        jira_client (JIRA): The Jira client object.
        issue (jira.resources.Issue): The Jira issue object.

    Returns:
        dict: A dictionary containing the relevant information from the Jira issue object.
    """
    if isinstance(issue, jira.resources.Issue):
        issue_id = issue.id
    else:
        issue_id = issue
    # print(issue_id)

    issue_info = jira_client.issue(issue_id)
    issue_info_raw = issue_info.raw['fields']

    # Define fields to export to snowflake
    if issue_info.fields.assignee is not None:
        issue_assignee = issue_info.fields.assignee.displayName
    else:
        issue_assignee = None

    # Get the story points of the issue
    issue_story_points = issue_info_raw.get("customfield_10014")

    # Get the epic link of the issue
    epic_link = issue_info_raw.get("customfield_11040")

    # Get the pair details of the issue
    pair_details = issue_info_raw.get("customfield_12185")
    if pair_details is not None:
        pair = pair_details['displayName']
    else:
        pair = None

    # Get the validator details of the issue
    validator_details = issue_info_raw.get("customfield_11140")
    if validator_details is not None:
        validator = validator_details['displayName']
    else:
        validator = None

    # Get the time in status of the issue
    time_in_status = issue_info_raw.get("customfield_10000")

    # Get the request type of the issue
    request_type = issue_info_raw.get("customfield_12101")

    # Get the start date of the issue
    start_date = issue_info_raw.get("customfield_12100")

    # Get the status of the issue
    issue_summary = issue_info.fields.summary

    # Get the description of the issue
    issue_desc = issue_info.fields.description

    # Get the issue type of the issue
    issue_type_name = issue_info.fields.issuetype.name

    # Get the labels of the issue
    labels = issue_info.fields.labels

    # Get the priority of the issue
    if issue_info.fields.priority is not None:
        priority = issue_info.fields.priority.name
    else:
        priority = None

    # Get the reporter of the issue
    if isinstance(issue_info.fields.reporter, str) or issue_info.fields.reporter is None:
        reporter = issue_info.fields.reporter
    else:
        reporter = issue_info.fields.reporter.displayName

    # Get the parent of the issue
    parent_details = issue_info_raw.get('parent')
    if parent_details is not None:
        parent = parent_details['key']
    else:
        parent = None

    # Get the due date of the issue
    due_date = issue_info_raw.get('duedate')
    program_codes = issue_info_raw.get('customfield_12162')
    if program_codes:
        program_codes = [code['value'] for code in program_codes]
    # Get the linked issues of the issue
    inward_issues = []
    outward_issues = []
    for linked in issue_info_raw.get('issuelinks'):
        if linked.get("outwardIssue") is not None:
            outward_issues.append(linked.get("outwardIssue")['key'])
        else:
            inward_issues.append(linked.get("inwardIssue")['key'])

    # Get the resolution date of the issue
    resolution_date = issue_info.fields.resolutiondate

    # Get the created on date of the issue
    created_on = issue_info.fields.created

    # Get the resolution of the issue
    if issue_info_raw.get("resolution") is not None:
        resolution = issue_info.fields.resolution.name
    else:
        resolution = None

    # Get the project of the issue
    project = issue_info.fields.project.key

    return {
        'project': project,
        # 'sprint_id': sprint.id,
        "issuetype": issue_type_name,
        "id": issue_id,
        "key": issue_info.key,
        "labels": labels,
        "summary": issue_summary,
        "description": issue_desc,
        'status': issue_info_raw['status']['name'],
        "assignee": issue_assignee,
        'story_points': issue_story_points,
        # "sprint": sprint.name,
        "epic_link": epic_link,
        "pair": pair,
        "validator": validator,
        "time_in_status": time_in_status,
        "request_type": request_type,
        "start_date": start_date,
        "priority": priority,
        "reporter": reporter,
        "parent": parent,
        "due_date": due_date,
        "resolution_date": resolution_date,
        "created_on": created_on,
        "resolution": resolution,
        # "linked_issues": linked_issues,
        "inward_issues": inward_issues,
        "outward_issues": outward_issues,
        'program_code': program_codes,
        # "subtasks": subtasks
    }


def get_issues(jira_client: JIRA, jql: str) -> pd.DataFrame:
    """
    Get all issues in a sprint
    This does NOT take into account the specific status of
    an issue at the duration of the sprint
    For example, an issue could be "Waiting for review" at the
    end of the sprint, but can be "Closed" now. This will
    skew the "number of story points" per engineer over time.

    Args:
        jira_client (JIRA): _description_
        sprint (jira.resources.Sprint): _description_

    Returns:
        pd.DataFrame: all issues in a sprint
    """
    all_results = []
    issues = jira_client.enhanced_search_issues(jql)
    all_results.extend(issues)
    while issues.nextPageToken:
        issues = jira_client.enhanced_search_issues(jql, nextPageToken=issues.nextPageToken)
        all_results.extend(issues)

    extracted_results = []
    for issue in all_results:
        issue_details = _extract_issue_details(jira_client=jira_client, issue=issue)
        # print(project)
        extracted_results.append(issue_details)
    jira_issues_df = pd.DataFrame(extracted_results)
    return jira_issues_df


def get_custom_headers(server: str='https://sagebionetworks.jira.com/', issue_id: str='BS-1') -> dict:
    """Map custom header names to readable names

    Args:
        server (str, optional): _description_. Defaults to 'https://sagebionetworks.jira.com/'.
        issue_id (str, optional): _description_. Defaults to 'BS-1'.

    Example:
        # Create dict with headers to update later
        headers = get_custom_headers(issue_id=WORKFLOWS-333)
        issues_with_headers = {}
        for key in issue_info.raw['fields'].keys():
            if 'customfield' in key:
                new_header = headers[key]
                issues_with_headers[new_header] = issue_info.raw['fields'][key]
        issue_info.raw['fields'].update(issues_with_headers)

    Returns:
        dict: Custom headers mapped to readable string headers
    """
    username = os.environ['JIRA_USERNAME']
    api_token = os.environ['JIRA_API_TOKEN']
    # base_url = 'https://sagebionetworks.jira.com/'
    auth = requests.auth.HTTPBasicAuth(username, api_token)
    headers = {
        "Accept": "application/json"
    }
    response = requests.get(f"{server}/rest/api/latest/issue/{issue_id}?expand=names", headers=headers, auth=auth)
    data = response.json()
    return data['names']



def main():
    """Invoke jira metrics
    """
    username = os.environ['JIRA_USERNAME']
    api_token = os.environ['JIRA_API_TOKEN']
    jira_client = JIRA(
        server='https://sagebionetworks.jira.com/',
        basic_auth=(username, api_token)
    )
    tech_roadmap_issues = get_issues(jira_client=jira_client, jql="project='Technology'")
    tech_roadmap_issues.to_csv("technology_issues.csv", index=False)
    # inward issues are delivery items
    all_delivery_issues = []
    for delivery_issue in tech_roadmap_issues['inward_issues']:
        all_delivery_issues.extend(delivery_issue)

    all_delivery_issue_information = []
    for delivery_issue_id in all_delivery_issues:
        all_delivery_issue_information.append(_extract_issue_details(jira_client=jira_client, issue=delivery_issue_id))

    all_delivery_issue_df = pd.DataFrame(all_delivery_issue_information)
    all_delivery_issue_df.to_csv("roadmap_delivery_issues.csv", index=False)
    all_issues_in_epic_df = pd.DataFrame()
    for _, issue in all_delivery_issue_df.iterrows():
        print(issue['key'], issue['summary'])
        if issue['issuetype'] == "Epic":
            issue_key = issue["key"]
            epic_issues = get_issues(jira_client=jira_client, jql=f"parent='{issue_key}'")
            # for epic_issue in epic_issues:
            #     all_issues_in_epic.append(_extract_issue_details(jira_client=jira_client, issue=epic_issue))
            if epic_issues.empty:
                continue
            all_issues_in_epic_df = pd.concat([all_issues_in_epic_df, epic_issues], ignore_index=True)
    # all_issues_in_epic_df = pd.DataFrame(all_issues_in_epic)
    all_issues_in_epic_df.to_csv("roadmap_epic_issues.csv", index=False)
    all_tech_roadmap_issues_df = pd.concat([all_delivery_issue_df, all_issues_in_epic_df, tech_roadmap_issues], ignore_index=True)

    date_columns = ['created_on', 'start_date', 'resolution_date']
    for date_column in date_columns:
        all_tech_roadmap_issues_df[date_column] = pd.to_datetime(all_tech_roadmap_issues_df[date_column], utc=True, errors='coerce').dt.strftime('%Y-%m-%d')
        # HACK: Error: Expression type does not match column data type, expecting DATE but got NUMBER(38,0) for column CREATED_ON
        # This error occurs but the `write_pandas` function seems to convert a column with empty values to a number type columns
        if all_tech_roadmap_issues_df[date_column].isnull().all():
            del all_tech_roadmap_issues_df[date_column]
        # tried these below, but they don't work
        # all_sprint_info[date_column].fillna(pd.NaT, inplace=True)
        # all_sprint_info[date_column] = all_sprint_info[date_column].astype('datetime64[ns]')
    all_tech_roadmap_issues_df['export_date'] = datetime.datetime.now(pytz.UTC).strftime('%Y-%m-%d')
    config = dotenv_values(".env")

    ctx = snowflake.connector.connect(
        user=config['user'],
        # password=config['password'],
        account=config['snowflake_account'],
        private_key_file=config['private_key_file'],
        private_key_file_pwd=config['private_key_file_pwd'],
        database="sage",
        schema="DPE",
        role="SYSADMIN",
        warehouse="compute_xsmall"
    )
    write_pandas(
        ctx,
        all_tech_roadmap_issues_df,
        "TECH_ROADMAP_JIRA_ISSUES",
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False
    )


if __name__ == "__main__":
    main()

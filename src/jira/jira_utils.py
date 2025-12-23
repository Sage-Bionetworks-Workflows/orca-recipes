import os
from typing import Union

from jira import JIRA
import jira
import pandas as pd
import requests


# This should be refactored.  There should be a function to extract the raw data and
# then separate functions to transform the Jira data based on the different projects
# as each project is set up differently which leads to differences in how the
# fields are being used.
def _extract_issue_details(
    jira_client: JIRA, issue: Union[str, jira.resources.Issue]
) -> dict:
    """
    Extracts the relevant information from a Jira issue object.

    This function takes a Jira issue object and extracts the relevant
    information from it. The information is then returned as a dictionary.

    Args:
        jira_client (JIRA): The Jira client object.
        issue (jira.resources.Issue|str): The Jira issue object or issue ID

    Returns:
        dict: A dictionary containing the relevant information from the Jira issue object.
    """
    if isinstance(issue, jira.resources.Issue):
        issue_id = issue.id
    else:
        issue_id = issue
    # print(issue_id)

    issue_info = jira_client.issue(issue_id)
    issue_info_raw = issue_info.raw["fields"]

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
        pair = pair_details["displayName"]
    else:
        pair = None

    # Get the validator details of the issue
    validator_details = issue_info_raw.get("customfield_11140")
    if validator_details is not None:
        validator = validator_details["displayName"]
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
    if (
        isinstance(issue_info.fields.reporter, str)
        or issue_info.fields.reporter is None
    ):
        reporter = issue_info.fields.reporter
    else:
        reporter = issue_info.fields.reporter.displayName

    # Get the parent of the issue
    parent_details = issue_info_raw.get("parent")
    if parent_details is not None:
        parent = parent_details["key"]
    else:
        parent = None

    # Get the due date of the issue
    due_date = issue_info_raw.get("duedate")
    program_codes = issue_info_raw.get("customfield_12162")
    if program_codes:
        program_codes = [code["value"] for code in program_codes]
    # Get the linked issues of the issue
    inward_issues = []
    outward_issues = []
    inward_is_implemented_by_issues = []
    for linked in issue_info_raw.get("issuelinks"):
        if linked.get("outwardIssue") is not None:
            outward_issues.append(linked.get("outwardIssue")["key"])
        # elif (
        #     linked.get("inwardIssue") is not None
        #     and linked["type"]["inward"] == "is implemented by"
        # ):
        #     inward_is_implemented_by_issues.append(linked.get("inwardIssue")["key"])
        elif linked.get("inwardIssue") is not None:
            inward_issues.append(linked.get("inwardIssue")["key"])
    
    sprint = issue_info_raw.get('customfield_10440')
    print(sprint)
    fix_version = issue_info_raw.get('fixVersions')
    print(fix_version)
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
        "project": project,
        'sprints': sprint,
        "issuetype": issue_type_name,
        "id": issue_id,
        "key": issue_info.key,
        "labels": labels,
        "summary": issue_summary,
        "description": issue_desc,
        "status": issue_info_raw["status"]["name"],
        "assignee": issue_assignee,
        "story_points": issue_story_points,
        "fix_version": fix_version,
        # "sprint": sprint.name,
        # "epic_link": epic_link,
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
        # These are delivery items for TECH roadmap (is implemented by)
        "inward_is_implemented_by_issues": inward_is_implemented_by_issues,
        "outward_issues": outward_issues,
        "program_code": program_codes,
    }


def get_issues(jira_client: JIRA, jql: str) -> pd.DataFrame:
    """
    Get all issues based on a JQL query

    Args:
        jira_client (JIRA): Logged in session of jira client
        jql (str): JQL query string

    Returns:
        pd.DataFrame: all issues matching the JQL query
    """
    all_results = []
    issues = jira_client.enhanced_search_issues(jql)
    all_results.extend(issues)
    while issues.nextPageToken:
        issues = jira_client.enhanced_search_issues(
            jql, nextPageToken=issues.nextPageToken
        )
        all_results.extend(issues)

    extracted_results = []
    for issue in all_results:
        print(issue.key)
        issue_details = _extract_issue_details(jira_client=jira_client, issue=issue)
        extracted_results.append(issue_details)
    jira_issues_df = pd.DataFrame(extracted_results)
    return jira_issues_df


def get_custom_headers(
    server: str = "https://sagebionetworks.jira.com/", issue_id: str = "BS-1"
) -> dict:
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
    username = os.environ["JIRA_USERNAME"]
    api_token = os.environ["JIRA_API_TOKEN"]
    # base_url = 'https://sagebionetworks.jira.com/'
    auth = requests.auth.HTTPBasicAuth(username, api_token)
    headers = {"Accept": "application/json"}
    response = requests.get(
        f"{server}/rest/api/latest/issue/{issue_id}?expand=names",
        headers=headers,
        auth=auth,
    )
    data = response.json()
    return data["names"]

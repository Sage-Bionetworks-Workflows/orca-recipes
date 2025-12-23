"""Summarize Jira ticket items per roadmap item using google gemini"""
import os

from google import genai
from google.genai import types
from jira import JIRA

from jira_utils import get_issues

# Given the following JIRA ticket descriptions generate a summary of the technology roadmap item for stakeholders. Only the results should be returned.**
PROMPT = """
### **Role:**
You are a Product Manager creating **stakeholder-facing release notes**.
### **Objective:**
Given a **JIRA issues**, produce a **concise, non-technical summary** that explains what's changing, why it matters, and who is impacted—at at the release level.
### **Requirements:**  

- **Always include `## Summary`**
  - purpose of the original TECH roadmap item
  - What is being delivered across epics
  - Why this work was prioritized
  - Expected impact on users, teams, or workflows

- **Synthesize, don't restate**
  - Abstract themes across epics and issues
  - Reference JIRA IDs only for traceability

- **Clearly call out**
  - Affected users, teams, and systems
  - Breaking or backward-incompatible changes (with mitigation steps and timelines)
  - Process or workflow changes and required actions
  - Required upgrades or migrations (with links if available)

- **Organize changes into**
  - `## New Features`
  - `## Fixes & Improvements`
  - `## Deprecated or Breaking Changes`

- **Tone**
  - Non-technical, concise, stakeholder-friendly

### **Input:**  
```
#### JIRA Tickets:
{JIRA_ISSUE_CONTENT}
```
### **Expected Output Format:**  
## Summary [stack release name]
[Brief high-level summary of what is changing and why]
[Who or what is impacted]
[Any important considerations for stakeholders]
[Number of tickets completed]
## Affected Users & Systems
## Deprecated or Breaking Changes
## New Features
"""
# stack-571
STACK_RELEASE = "stack-570"
client = genai.Client()


def construct_summary(jira_issue_content: dict[str, str]) -> None:
    """
    Prompt the agent with summary prompt based on Jira ticket descriptions
    """
    prompt = PROMPT.format(
        JIRA_ISSUE_CONTENT=jira_issue_content,
    )
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0)  # Disables thinking
        ),
    )
    return response.text


def main():
    """main workflow"""
    username = os.environ["JIRA_USERNAME"]
    api_token = os.environ["JIRA_API_TOKEN"]
    jira_client = JIRA(
        server="https://sagebionetworks.jira.com/", basic_auth=(username, api_token)
    )
    stack_release_issues_df = get_issues(
        jira_client=jira_client,
        jql=f'resolution IS NOT EMPTY AND project in (PORTALS, SWC, PLFM) AND issuetype != Epic AND fixVersion = {STACK_RELEASE} ORDER BY created DESC',
    )

    # tech_roadmap_responses = []
    jira_issue_content = {}
    for _, issue_row in stack_release_issues_df.iterrows():
        # If we want more information to be summarized, we can add more metadata
        metadata = {
            "summary": issue_row["summary"],
            "description": issue_row["description"],
            # 'program_code': issue_row['program_code'],
            # "assignee": issue_row["assignee"],
            "issuetype": issue_row["issuetype"],
            "fix_version": issue_row["fix_version"],
        }
        jira_issue_content[issue_row["key"]] = metadata

    response = construct_summary(jira_issue_content)
    print(response)
    # tech_roadmap_responses.append(response)
    # done_items["AI_summary_of_roadmap_item"] = tech_roadmap_responses
    # done_items.to_csv("technology_issues_with_AI_summary.csv", index=False)

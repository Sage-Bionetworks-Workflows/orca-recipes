"""Summarize Jira ticket items per roadmap item using google gemini.

1. Obtain Google GEMINI API key (Work with sage IT) and set it in the environment variable GEMINI_API_KEY
2. Obtain Jira API token and set it in the environment variables JIRA_USERNAME and JIRA_API_TOKEN.
   Your username is your sage email.
3. Install dependencies: `pip install -r requirements.txt`.  This is assuming you have an understanding
   of how to set up your Python environment.

Usage:
python platform_release_notes.py stack-570
"""
import argparse
import os

from atlassian import Confluence
from google import genai
from google.genai import types
from jira import JIRA

from jira_utils import get_issues, get_or_create_page

# Given the following JIRA ticket descriptions generate a summary of the technology roadmap item for stakeholders. Only the results should be returned.**
PROMPT = """
### **Role:**
You are a Product Manager creating **stakeholder-facing release notes**.
### **Objective:**
Given a **JIRA issues**, produce a **concise, non-technical summary** that explains what's changing, why it matters, and who is impacted—at at the release level.
### **Requirements:**  

- **Hard formatting constraints (non-negotiable):**
  - The curly braces are forbidden in the output.
  - Any API path parameters MUST be represented using angle brackets (e.g., `<sessionId>`).
  - Before returning the final answer, validate that the output contains zero curly braces characters. If any are present, rewrite the output until none remain.

- The document MUST be written using Confluence wiki markup only (e.g., `h1.`, `h2.`, `h3.`).
- In the sections for New Features and Fixes & Improvements, each item MUST be `h3.` or `h4.`.
- **Source-of-truth rule:**
  - Only summarize information explicitly present in the provided JIRA content.
  - If details are missing, omit them rather than infer or speculate.

- **Always include `h1. Summary`**
  - purpose of the release based on the JIRA issues
  - What is being delivered
  - Why this work was prioritized
  - Expected impact on users, teams, or workflows

- **Synthesize, don't restate**
  - Abstract themes across issues
  - Reference JIRA IDs only for traceability

- **Clearly call out**
  - Affected users, teams, and systems
  - Breaking or backward-incompatible changes (with mitigation steps and timelines)
  - Process or workflow changes and required actions
  - Required upgrades or migrations (with links if available)

- **Organize changes into**
  - `h1. New Features`
  - `h1. Fixes & Improvements`
  - `h1. Deprecated or Breaking Changes`

- **Tone**
  - Non-technical, concise, stakeholder-friendly

### **Input:**  
```
#### JIRA Tickets:
{JIRA_ISSUE_CONTENT}
```
### **Expected Output Format:**  
h1. Summary [stack release name]
[Brief high-level summary of what is changing and why]
[Who or what is impacted]
[Any important considerations for stakeholders]
[Number of tickets completed]
h1. Affected Users & Systems
h1. Deprecated or Breaking Changes
h1. New Features
"""


def construct_summary(jira_issue_content: dict[str, str]) -> None:
    """
    Prompt the agent with summary prompt based on Jira ticket descriptions
    """
    client = genai.Client()
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


def main(stack_release: str):
    """main workflow"""
    username = os.environ["JIRA_USERNAME"]
    api_token = os.environ["JIRA_API_TOKEN"]
    jira_client = JIRA(
        server="https://sagebionetworks.jira.com/", basic_auth=(username, api_token)
    )
    stack_release_issues_df = get_issues(
        jira_client=jira_client,
        jql=f'resolution IS NOT EMPTY AND project in (PORTALS, SWC, PLFM) AND issuetype != Epic AND fixVersion = {stack_release} ORDER BY created DESC',
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
    confluence = Confluence(
        url="https://sagebionetworks.jira.com/wiki",
        username=username,
        password=api_token
    )
    news_page_confluence_id = 4467982339
    page_id = get_or_create_page(
        space="DRAFT", client=confluence, title=f"{stack_release} Release Notes", parent_id=news_page_confluence_id
    )
    response += "\n\n\n> These release notes are auto generated by Google GEMINI so responses may not be completely accurate"
    confluence.update_page(
        page_id=page_id,
        title=f"{stack_release} Release Notes",
        body=response,
        parent_id=news_page_confluence_id,
        representation='wiki'
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Generate platform release notes from JIRA tickets')
    parser.add_argument(
        'stack_release',
        type=str,
        help='Stack release version (e.g., stack-570)'
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    main(args.stack_release)

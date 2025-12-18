# import base64
# import json
# import re
from typing import Dict, Set
# import httpx
import pandas as pd
from synapseclient import Synapse
from synapseclient.models import Agent

import ast

PROMPT = """
**Given the following JIRA ticket descriptions generate a summary of the technology roadmap item for stakeholders. Only the results should be returned.**  
### **Requirements:**  
- **Always include a `## Summary` section** that provides a high-level, **non-technical** overview of the roadmap item. This should include:  
  - What has changed.  
  - Why these changes were made.  
  - The expected impact on users, teams, or workflows.  
- **Affected Users/Systems:** Clearly identify which teams, applications, or workflows are impacted by this roadmap item.  
- **Breaking Changes:** Explicitly call out any backward-incompatible changes. If there are breaking changes, include:  
  - What is changing and why.  
  - Steps required to adapt to the change.  
  - Any timelines or deprecation notices.  
- **Process Changes:** If any operational, workflow, or permission-related changes have been introduced, provide clear guidance on:  
  - What has changed.  
  - How stakeholders need to adjust.  
  - Any required actions.  
- **Upgrade & Migration Steps:** If users need to update configurations, dependencies, or SDKs, include:  
  - The required upgrade steps.  
  - Links to documentation or additional resources.  
- **Categorize changes into `## New Features`, `## Fixes & Improvements`, and `## Deprecated or Breaking Changes` as appropriate.**  
  - Each section should have **bullet points summarizing key changes**.  
  - **JIRA ticket IDs** should be included where relevant but not verbatim descriptions.  
### **Input:**  
```
#### JIRA Ticket Descriptions:
{JIRA_ISSUE_CONTENT}

```
### **Expected Output Format:**  
```  
## Summary  
- [Brief high-level summary of what is changing and why]  
- [Who or what is impacted]  
- [Any important considerations for stakeholders]  
## Affected Users & Systems  
- [List of impacted teams, applications, or workflows]  
## Breaking Changes  
- [List of backward-incompatible changes]  
- [Steps required to mitigate these changes]  
- [Timelines for migration, if applicable]  
## Process Changes  
- [Description of workflow, operational, or permission updates]  
- [Actions required by stakeholders]  
## New Features  
- [List of new features with impact on stakeholders]  
## Fixes & Improvements  
- [List of bug fixes and general improvements]  
## Deprecations
- [List of deprecated features, APIs, or configurations]  
- [Recommended migration path]  
For more details, visit [TECH ROADMAP ITEM](Tech roadmap item jira link)  
```
"""
syn = Synapse()
syn.login()
# It is important to use registration_id="139" because this is the dpe-technical-writer agent
release_note_agent = Agent(registration_id="139").get()


def construct_release_notes(jira_issue_content: Dict[str, str]) -> None:
    """
    Prompt the agent with the release notes template and the extracted
    JIRA and Github content.
    """
    prompt = PROMPT.format(
        JIRA_ISSUE_CONTENT=jira_issue_content,
    )
    results = release_note_agent.prompt(prompt=prompt)
    return results.response


def main():
    technology_jira_issues = pd.read_csv("technology_issues.csv")
    roadmap_jira_issues = pd.read_csv("roadmap_delivery_issues.csv")
    roadmap_epic_issues = pd.read_csv("roadmap_epic_issues.csv")
    done_items = technology_jira_issues[technology_jira_issues['status'] == "Done"]
    tech_roadmap_responses = []
    for _, row in done_items.iterrows():
        print(row)
        # issue_id = row['issue_id']
        delivery_tickets = ast.literal_eval(row['inward_issues'])
        subset_issues = roadmap_jira_issues[roadmap_jira_issues['id'].isin(delivery_tickets)]
        all_epic_issues = roadmap_epic_issues[roadmap_epic_issues['parent'].isin(subset_issues.id)]
        all_issues = pd.concat([subset_issues, all_epic_issues, row.to_frame().T], ignore_index=True)
        jira_issue_content = {}
        for _, issue_row in all_issues.iterrows():
            print(issue_row['summary'])
            jira_issue_content[issue_row['key']] = {
                'summary': issue_row['summary'],
                'description': issue_row['description'],
                # 'program_code': issue_row['program_code'],
            }

        response = construct_release_notes(jira_issue_content)
        tech_roadmap_responses.append(response)
    technology_jira_issues['AI_summary_of_roadmap_item'] = tech_roadmap_responses
    technology_jira_issues.to_csv("technology_issues_with_AI_summary.csv", index=False)

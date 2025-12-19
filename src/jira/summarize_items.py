# import base64
# import json
# import re
from typing import Dict, Set
# import httpx
import pandas as pd
from synapseclient import Synapse
from synapseclient.models import Agent

import ast
# Given the following JIRA ticket descriptions generate a summary of the technology roadmap item for stakeholders. Only the results should be returned.**  
PROMPT = """
### **Role:**
You are a Product Manager creating a **stakeholder-facing roadmap update**.
### **Objective:**
Given a **roadmap item** with linked **epics and JIRA issues**, produce a **concise, non-technical summary** that explains what’s changing, why it matters, and who is impacted—at the **roadmap item level**.
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
## Summary
[Purpose of the original TECH roadmap item]
[Brief high-level summary of what is changing and why]
[Who or what is impacted]
[Any important considerations for stakeholders]
## Affected Users & Systems
## Deprecated or Breaking Changes
## New Features
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
        # issue_id = row['issue_id']
        delivery_tickets = ast.literal_eval(row['inward_issues'])
        subset_issues = roadmap_jira_issues[roadmap_jira_issues['id'].isin(delivery_tickets)]
        all_epic_issues = roadmap_epic_issues[roadmap_epic_issues['parent'].isin(subset_issues.id)]
        all_issues = pd.concat([subset_issues, all_epic_issues, row.to_frame().T], ignore_index=True)
        all_issues = all_issues[~all_issues['resolution'].isin(["Won't Do", "Duplicate", "Cancelled", "Cannot Reproduce", "Won't Fix", "Incomplete", "Known Error"])]
        print(row['key'], row['summary'], f"- {sum(~all_issues['issuetype'].isin(['Epic', 'Idea']))} issues")

        jira_issue_content = {}
        for _, issue_row in all_issues.iterrows():
            metadata = {
                'summary': issue_row['summary'],
                # 'program_code': issue_row['program_code'],
            }
            if not pd.isna(issue_row['description']):
                metadata['description'] = issue_row['description']
            jira_issue_content[issue_row['key']] = metadata

        response = construct_release_notes(jira_issue_content)
        print(response)
        tech_roadmap_responses.append(response)
    technology_jira_issues['AI_summary_of_roadmap_item'] = tech_roadmap_responses
    technology_jira_issues.to_csv("technology_issues_with_AI_summary.csv", index=False)

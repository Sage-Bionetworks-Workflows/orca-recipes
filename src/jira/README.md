# JIRA 

A JIRA workflow for fetching Jira issues within Sage Bionetworks and pushing into Snowflake

## Prerequisites

- A JIRA API Token https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/
- A Google Gemini API key https://aistudio.google.com/
- Access to Sage Bionetworks JIRA instance
- Snowflake credentials for data ingestion https://sagebionetworks.jira.com/wiki/spaces/DPE/pages/4229300245/Programmatic+Authentication+in+Snowflake

## Usage instructions

1. Set up environment variables in a `.env` file or export them in your shell:

    ```
    snowflake_account = ...
    user = "YOUR_USER"
    authenticator = "SNOWFLAKE_JWT"
    private_key_file = "...private_key.p8"
    private_key_file_pwd = "..."
    ```

2. Set up your env variables for Jira and google gemini access

    ```
    export GEMINI_API_KEY=...
    export JIRA_USERNAME=...@sagebase.org
    export JIRA_API_TOKEN=...
    ```

3. Ensure the above packages are installed in your environment:

    ```
    pip install -r requirements.txt
    ```

4. Run the script to fetch Jira issues and push them to Snowflake:

    ```
    python jira_metrics.py
    ```

5. Run the summarization script to generate summaries using Google Gemini:

    ```
    python jira_summarizer.py
    ```

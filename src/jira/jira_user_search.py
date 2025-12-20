"""Extract all Jira users via REST API and load into Snowflake."""
# NOTE: This script requires a lot of work to be production ready
import os
import sys
import requests
import pandas as pd
from dotenv import dotenv_values
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# This script needs to be heavily refactored
JIRA_BASE_URL = "https://sagebionetworks.jira.com/"
JIRA_EMAIL = os.environ.get("JIRA_USERNAME")  # Atlassian account email used for auth
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")  # API token from id.atlassian.com

if not (JIRA_BASE_URL and JIRA_EMAIL and JIRA_API_TOKEN):
    print("Set env vars: JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN", file=sys.stderr)
    sys.exit(1)
session = requests.Session()
session.auth = (JIRA_EMAIL, JIRA_API_TOKEN)
session.headers.update({"Accept": "application/json"})

all_users = []
start_at = 0
max_results = 1000

# Refactor this loop
while True:
    url = f"{JIRA_BASE_URL}/rest/api/3/user/search"
    params = {
        "query": "",  # mirrors your ScriptRunner queryString("query","")
        "startAt": start_at,
        "maxResults": max_results,
    }
    r = session.get(url, params=params, timeout=60)

    if r.status_code != 200:
        raise RuntimeError(f"Error: {r.status_code} - {r.text}")

    users = r.json()
    if not users:
        break

    all_users.extend(users)
    start_at += len(users)

    if len(users) < max_results:
        break


all_users_df = pd.DataFrame(all_users)
# Remove extraneous fields
del all_users_df["self"]
del all_users_df["avatarUrls"]

all_users_df.to_csv("all_jira_users.csv", index=False)


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
    all_users_df,
    "USERS",
    auto_create_table=True,
    overwrite=True,
    quote_identifiers=False,
)

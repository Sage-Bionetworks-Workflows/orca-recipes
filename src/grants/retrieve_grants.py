from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import re

import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import synapseclient
from synapseclient import Synapse
from synapseclient.models import File

syn = Synapse()
syn.login(silent=True)


def step1_a_retrieve_federal_grants(api_url: str, request_body: dict) -> dict:
    """Retrieve federal grant records from a remote API endpoint.

    Sends a POST request with a JSON payload to the specified API endpoint
    and returns the value of the ``data`` field from the JSON response.

    Args:
        api_url (str): The full URL of the API endpoint used to retrieve
            federal grant data.
        request_body (Dict[str, Any]): A JSON-serializable dictionary
            containing the request payload (e.g., filters, pagination,
            or query parameters required by the API).

    Returns:
        dict: The contents of the
        ``"data"`` field from the API response. The exact structure depends
        on the API but typically contains federal grant records.
    """
    headers = {"Content-Type": "application/json"}

    response = requests.post(api_url, json=request_body, headers=headers)

    return response.json()["data"]


def step1_b_retrieve_non_federal_grants_from_synapse() -> pd.DataFrame:
    """
    Retrieves grants data from a Synapse CSV file. This data will be in the same format as the grants schema>
    """
    syn = Synapse()
    syn.login(silent=True)

    file = File("syn72004505").get(synapse_client=syn)
    df = pd.read_csv(file.path, sep=",")
    return df


def step2_preliminary_filter(grants_data: dict):
    """
    Filters by:
    1. Closing date for grant proposal
    TODO: add filter for award minimum
    """
    hits = grants_data["oppHits"]

    month_from_now = datetime.today().date() + relativedelta(months=1)

    eligible_grants = []
    for h in hits:
        if not h["closeDate"]:
            continue
        print("processing grant...")
        print(f"{h['id']}: {h['title']}")
        close_date = datetime.strptime(h["closeDate"], "%m/%d/%Y").date()
        print(close_date)
        if not close_date <= month_from_now:
            print("this grant is eligible")
            eligible_grants.append(h)
    print(eligible_grants)
    print("final eligible grants for keyword:", len(eligible_grants))
    print("\n")
    return eligible_grants


def fetch_opportunity_details(opportunity_id: int):
    """Fetch opportunity details from Grants.gov API"""
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        "https://api.grants.gov/v1/api/fetchOpportunity",
        headers=headers,
        json={"opportunityId": opportunity_id},
    )
    return response.json()["data"]


def extract_opportunity_fields(opportunity_details: dict):
    """Filter opportunity details to get the required fields based on the data model"""
    synopsis = opportunity_details.get("synopsis", {})

    # Handle awardFloor - can be "none" or a number
    award_floor = synopsis.get("awardFloor", "none")
    if award_floor == "none" or award_floor is None:
        funding_amount = None
    else:
        try:
            funding_amount = int(award_floor)
        except (ValueError, TypeError):
            funding_amount = None

    # Parse dates
    post_date = synopsis.get("postingDate")
    end_date = synopsis.get("responseDate")

    # Calculate duration in years if both dates exist
    grant_duration = None
    if post_date and end_date:
        try:
            print("POST")
            print(post_date, end_date)
            post_dt = datetime.strptime(post_date, "%m/%d/%Y")
            end_dt = datetime.strptime(end_date, "%m/%d/%Y")
            grant_duration = (end_dt - post_dt).days / 365.25
        except (ValueError, TypeError):
            grant_duration = None

    # Create DataFrame from dictionary (single row) - values must be in lists

    print("funding amount:", funding_amount)
    converted_funding_amount = (
        re.sub(r"\D", "", str(funding_amount)) if funding_amount is not None else None
    )
    print("converted funding amount:", converted_funding_amount)
    # TODO: Figure out why Snowflake only recognizes the columns when you make it a string (e.g. select "title" works but select title does not)
    data = {
        "id": opportunity_details["id"],
        "title": [opportunity_details.get("opportunityTitle")],
        "funding_amount": [converted_funding_amount],
        "organization": [synopsis.get("agencyContactName")],
        "contact_info": [synopsis.get("agencyContactEmail")],
        "data_source": ["grants.gov"],
        "post_date": [post_date],
        "end_date": [end_date],
        "grant_description": [synopsis.get("synopsisDesc")],
        "grant_duration": [grant_duration],
        "domain": ["TBD"],
    }

    opportunity_details_df = pd.DataFrame(data)
    return opportunity_details_df


def step3_append_grant_details(grants_data: dict):
    """Append grant details to the grants data."""
    grants_df = pd.DataFrame()
    for grant in grants_data:
        opportunity_id = grant["id"]
        grant_details = fetch_opportunity_details(opportunity_id)
        grant_details = extract_opportunity_fields(grant_details)
        grants_df = pd.concat([grants_df, grant_details], ignore_index=True)
    return grants_df


def step4_upload_to_snowflake(
    table_df, table_name, auto_table_create=False, overwrite=False
):
    """
    Uploads the final table of the pipeline to Snowflake.
    TODO: Replace this with Rixing's snowflake_utils module when that's merged
    TODO: Update credentials to use a service user account instead of personal user account
    """
    # conn = snowflake.connector.connect(
    #     account="mqzfhld-vp00034",
    #     user="jenny.medina@sagebase.org",
    #     password=os.getenv("SNOWFLAKE_PAT"),
    #     role="DATA_ENGINEER",
    #     warehouse="COMPUTE_XSMALL",
    #     database="SAGE",
    #     schema="DPE",
    # )
    conn = snowflake.connector.connect(
        account='mqzfhld-vp00034',
        user=os.getenv("SNOW_USER"),
        private_key_file=os.getenv("SNOW_PRIVATE"),
        private_key_file_pwd=os.getenv("SNOW_PRIVATE_PWD"),
        database="SAGE",
        schema="GRANTS",
        role="DATA_ENGINEER",
        warehouse="compute_xsmall",
    )
    write_pandas(
        conn,
        table_df,
        table_name,
        auto_create_table=auto_table_create,
        overwrite=overwrite,
        quote_identifiers=False,
    )


def all_federal_grants(api_url, request_body):
    federal_grants = step1_a_retrieve_federal_grants(api_url, request_body)
    eligible_grants = step2_preliminary_filter(federal_grants)
    federal_grants = step3_append_grant_details(eligible_grants)
    return federal_grants


if __name__ == "__main__":
    limit_rows = 10
    kwds = [
        "alzheimer",
        "arthritis",
        "autoimmune",
        "cancer",
        "nf",
        "neurofibromatosis",
        "longevity",
        "elite",
        "data coordinating center",
        "data management",
        "open science",
    ]
    api_url = "https://api.grants.gov/v1/api/search2"

    all_fedral_grants = pd.DataFrame()
    for keyword in kwds:
        print("Searching for grants with keyword:", keyword)
        body = {
            "rows": limit_rows,
            "keyword": keyword,
            "eligibilities": "",
            "agencies": "",
            "oppStatuses": "forecasted|posted",
            "aln": "",
            "fundingCategories": "",
        }
        grants = all_federal_grants(api_url, body)
        all_fedral_grants = pd.concat([all_fedral_grants, grants], ignore_index=True)

    non_federal_grants = step1_b_retrieve_non_federal_grants_from_synapse()
    all_grants = pd.concat([all_fedral_grants, non_federal_grants])
    print("ALL GRANTS:", len(all_grants))
    print(all_grants)
    table_df = pd.DataFrame(all_grants)
    print("converted to dataframe:")
    print(table_df)
    print("Uploading to Snowflake...")
    step4_upload_to_snowflake(
        table_df, "GRANTS_PIPELINE_TEST", auto_table_create=True, overwrite=True
    )

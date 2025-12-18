import os
import pandas as pd
import requests
import snowflake.connector
from datetime import datetime
from dateutil.relativedelta import relativedelta
from snowflake.connector.pandas_tools import write_pandas
import synapseclient
from synapseclient import Synapse
from synapseclient.models import File


def step1_retrieve_grants(api_url, request_body):

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(api_url, json=request_body, headers=headers)

    return response.json()['data']

def step1_b_retrieve_grants_from_synapse() -> pd.DataFrame:
    """
    Retrieves grants data from a Synapse CSV file. This data will be in the same format as the grants schema>
    """
    syn = Synapse()
    syn.login(silent=True)

    file = File("syn72004505").get(synapse_client=syn)
    df = pd.read_csv(file.path, sep=",")
    return df

def step2_preliminary_filter(grants_data):
    """
    Filters by:
    1. Closing date for grant proposal
    TODO: add filter for award minimum
    """
    hits = grants_data['oppHits']
    
    month_from_now = datetime.today().date() + relativedelta(months=1)
    
    eligible_grants = []
    for h in hits:
        if not h['closeDate']:
            continue
        print('processing grant...')
        print(f"{h['id']}: {h['title']}")
        close_date = datetime.strptime(h['closeDate'], "%m/%d/%Y").date()
        print(close_date)
        if not close_date <= month_from_now:
            print('this grant is eligible')
            eligible_grants.append(h)
    print(eligible_grants)
    print("final eligible grants for keyword:", len(eligible_grants))
    print('\n')
    return eligible_grants


def step3_upload_to_snowflake(table_df, table_name, auto_table_create=False, overwrite=False):
    """
    Uploads the final table of the pipeline to Snowflake.
    TODO: Replace this with Rixing's snowflake_utils module when that's merged
    TODO: Update credentials to use a service user account instead of personal user account
    """
    conn = snowflake.connector.connect(
                account='mqzfhld-vp00034',
                user='jenny.medina@sagebase.org',
                password=os.getenv("SNOWFLAKE_PAT"),
                role='DATA_ENGINEER',
                warehouse='COMPUTE_XSMALL',
                database='SAGE',
                schema='DPE'
            )
    
    write_pandas(conn, table_df, table_name, auto_create_table=auto_table_create, overwrite=overwrite)


def retrieve_federal_grants(api_url, request_body):
    grants_data = step1_retrieve_grants(api_url, request_body)
    eligible_grants = step2_preliminary_filter(grants_data)
    return eligible_grants


if __name__ == "__main__":
    limit_rows = 10
    kwds = ['alzheimer', 'arthritis', 'autoimmune', 'cancer', 'nf', 'neurofibromatosis', 'longevity', 'elite']
    api_url = "https://api.grants.gov/v1/api/search2"

    all_grants = []
    for keyword in kwds:
        print("Searching for grants with keyword:", keyword)
        body = {
                "rows": limit_rows,
                "keyword": keyword,
                "eligibilities": "",
                "agencies": "",
                "oppStatuses": "forecasted|posted",
                "aln": "",
                "fundingCategories": ""
            }
        grants = retrieve_federal_grants(api_url, body)
        all_grants.extend(grants)
    
    print("ALL GRANTS:", len(all_grants))
    print(all_grants)
    table_df = pd.DataFrame(all_grants)
    print("converted to dataframe:")
    print(table_df)
    print("Uploading to Snowflake...")
    #step3_upload_to_snowflake(table_df, auto_table_create=True, overwrite=True)
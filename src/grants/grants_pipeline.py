import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta


def step1_retrieve_grants(api_url, request_body):

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(api_url, json=request_body, headers=headers)

    return response.json()['data']

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
            
    return eligible_grants

def grants_pipeline(api_url, request_body):
    grants_data = step1_retrieve_grants(api_url, request_body)
    eligible_grants = step2_preliminary_filter(grants_data)
    return eligible_grants


if __name__ == "__main__":
    limit_rows = 10
    kwds = ['alzheimer', 'arthritis', 'autoimmune', 'cancer', 'nf', 'neurofibromatosis', 'longevity', 'elite']
    api_url = "https://api.grants.gov/v1/api/search2"

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
        print(grants_pipeline(api_url, body))
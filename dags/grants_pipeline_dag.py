from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param

# Import your existing functions
# from your_module import grants_pipeline, step3_upload_to_snowflake


@dag(
    dag_id="grants_gov_search_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # or a cron like "0 6 * * *"
    catchup=False,
    default_args={"owner": "sage", "retries": 1},
    params={
        "limit_rows": Param(10, type="integer", minimum=1),
        "kwds": Param(
            [
                "alzheimer",
                "arthritis",
                "autoimmune",
                "cancer",
                "nf",
                "neurofibromatosis",
                "longevity",
                "elite",
            ],
            type="array",
            items={"type": "string"},
        ),
        "api_url": Param("https://api.grants.gov/v1/api/search2", type="string"),
        "overwrite": Param(True, type="boolean"),
        "auto_table_create": Param(True, type="boolean"),
    },
    tags=["grants.gov", "snowflake"],
)
def grants_gov_search_to_snowflake():
    @task
    def build_request_bodies(
        kwds: List[str], limit_rows: int, api_url: str
    ) -> List[Dict[str, Any]]:
        bodies: List[Dict[str, Any]] = []
        for keyword in kwds:
            bodies.append(
                {
                    "api_url": api_url,
                    "body": {
                        "rows": limit_rows,
                        "keyword": keyword,
                        "eligibilities": "",
                        "agencies": "",
                        "oppStatuses": "forecasted|posted",
                        "aln": "",
                        "fundingCategories": "",
                    },
                }
            )
        return bodies

    @task
    def fetch_grants_for_keyword(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        api_url = payload["api_url"]
        body = payload["body"]
        keyword = body.get("keyword", "<unknown>")
        print("Searching for grants with keyword:", keyword)
        grants = grants_pipeline(api_url, body)
        return grants

    @task
    def combine_and_upload(
        grants_by_keyword: List[List[Dict[str, Any]]],
        auto_table_create: bool,
        overwrite: bool,
    ) -> int:
        # Flatten
        all_grants: List[Dict[str, Any]] = [
            g for sublist in grants_by_keyword for g in (sublist or [])
        ]
        print("ALL GRANTS:", len(all_grants))

        table_df = pd.DataFrame(all_grants)
        print("converted to dataframe:")
        print(table_df.head())

        print("Uploading to Snowflake...")
        step3_upload_to_snowflake(
            table_df,
            auto_table_create=auto_table_create,
            overwrite=overwrite,
        )
        return len(all_grants)

    # Wire it up (use DAG params)
    bodies = build_request_bodies(
        kwds="{{ params.kwds }}",
        limit_rows="{{ params.limit_rows }}",
        api_url="{{ params.api_url }}",
    )
    grants_lists = fetch_grants_for_keyword.expand(payload=bodies)
    combine_and_upload(
        grants_by_keyword=grants_lists,
        auto_table_create="{{ params.auto_table_create }}",
        overwrite="{{ params.overwrite }}",
    )


dag = grants_gov_search_to_snowflake()

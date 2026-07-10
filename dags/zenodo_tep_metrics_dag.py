"""Zenodo TREAT-AD Target Enabling Package (TEP) Metrics DAG

This DAG automates the monthly collection of TEP usage metrics (views and
downloads) from the Zenodo API for the TREAT-AD community and exports them to
Synapse. The DAG follows these steps:

1. Fetch the TREAT-AD community's records from the Zenodo API using an
   authentication token stored in Airflow Variables (paginated, with Airflow
   retries handling transient/busy API responses).
2. Validate the pulled metrics against an expected schema to ensure stability of
   the Zenodo API response and the computed aggregates. If the schema has
   changed or a value is missing, the task logs the problem and fails so that
   Airflow retries kick in and the failure alert is sent.
3. Export the metrics to CSV files and upload them to a Synapse
   folder. The reports will contain the following variables:
    - "Title"
    - "Publication Date"
    - "Total Views"
    - "Unique Views"
    - "Total Downloads"
    - "Unique Downloads"
    - "Link"
   Also includes a totals row for each metric.
4. Notify the collaborator(s) via email that a new report is available.

On any task failure (after retries are exhausted), a Synapse message is sent to
the DPE/developer list via the DAG's on_failure_callback so that schema
changes or other breakages are surfaced.

The DAG runs monthly. The Zenodo API token is stored in the Airflow secrets
backend as the ZENODO_API_TOKEN Variable.
"""

import csv
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, NamedTuple

from airflow.decorators import dag, task
from airflow.models import DAG as AirflowDAG, Param, Variable
import requests
from synapseclient.models import File

from src.synapse_hook import SynapseHook
from src.synapse_alerts import send_synapse_message, synapse_failure_callback

logger = logging.getLogger(__name__)

# Zenodo communities API endpoint. Records are pulled from the community's
# records collection: {ZENODO_COMMUNITIES_URL}/{community_id}/records
ZENODO_COMMUNITIES_URL = "https://zenodo.org/api/communities"

# Zenodo community whose records we pull (the TREAT-AD TEP community)
TREATAD_COMMUNITY_ID = "treatad"

# Fields we expect on every processed record. Used by the validation task to
# detect Zenodo API schema drift
REQUIRED_RECORD_FIELDS = [
    "title",
    "date",
    "views",
    "unique_views",
    "downloads",
    "unique_downloads",
    "link",
    "category",
]

# Numeric metric fields that must be non-negative integers
METRIC_FIELDS = ["views", "unique_views", "downloads", "unique_downloads"]

# Headers for the Excel report (in the order they appear in the workbook)
EXCEL_HEADERS = [
    "Title",
    "Publication Date",
    "Total Views",
    "Unique Views",
    "Total Downloads",
    "Unique Downloads",
    "Link",
]

dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    # Synapse folder/project the Excel report is uploaded to. Update to the
    # shared TREAT-AD reporting location.
    "synapse_export_folder": Param("syn75951837", type="string"),
    # Comma-separated Synapse usernames of collaborator(s) to notify when the
    # monthly report is available. (No support for python lists in Params.)
    "collaborator_user_list": Param("3460442", type="string"),
    # Comma-separated Synapse usernames or IDs of DPE/developers to
    # alert on failure.
    "dev_user_list": Param("3460442", type="string"),
}

dag_config = {
    # Run on the first day of the month at midnight
    "schedule": "0 0 1 * *",
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
    "default_args": {
        "retries": 3,
    },
    "tags": ["zenodo", "treat-ad"],
    "params": dag_params,
}


def fetch_tep_records(
    api_token: str,
    community_id: str = TREATAD_COMMUNITY_ID,
) -> List[Dict[str, Any]]:
    """Fetch and process the TREAT-AD community's records from Zenodo.

    Pulls every record in the given Zenodo community (handling pagination) and
    truncates each record down to the metric fields of interest. Each record is
    tagged with a category of "report" (Package/Report) or "component"
    (Component/Resource) based on its title.
    
    The search will stop when either:
      1) The API returns an empty page of results, or
      2) The number of records pulled is greater than or equal to the total
         number of records reported by the API, or
      3) The number of records returned on the current page is less than the
         requested page size (indicating the last page).

    Arguments:
        api_token (str): Zenodo API token
        community_id (str): Zenodo community whose records to pull

    Returns:
        List[Dict[str, Any]]: Processed TEP records

    Raises:
        requests.exceptions.RequestException: If a Zenodo API request fails, this
            is intentionally allowed to propagate so Airflow retries handle a
            busy/transient API.
    """
    headers = {"Authorization": f"Bearer {api_token}"}
    community_url = f"{ZENODO_COMMUNITIES_URL}/{community_id}/records"

    teps: List[Dict[str, Any]] = []
    params: Dict[str, Any] = {"size": 100, "page": 1}
    while True:
        logger.info(f"Fetching page {params['page']} of community '{community_id}'")
        response = requests.get(
            community_url, params=params, headers=headers, timeout=30
        )
        response.raise_for_status()
        data = response.json()

        hits = data["hits"]["hits"]
        if not hits:
            break

        for record in hits:
            title = record["metadata"]["title"]
            stats = record.get("stats", {})
            teps.append(
                {
                    "title": title,
                    "date": record["metadata"]["publication_date"],
                    "views": stats.get("views", 0),
                    "unique_views": stats.get("unique_views", 0),
                    "downloads": stats.get("downloads", 0),
                    "unique_downloads": stats.get("unique_downloads", 0),
                    "link": f"https://zenodo.org/records/{record['id']}",
                    "category": categorize_title(title),
                }
            )
        logger.info(f"  Retrieved {len(hits)} records")

        total_records = data["hits"]["total"]
        if len(teps) >= total_records or len(hits) < params["size"]:
            break
        params["page"] += 1

    logger.info(f"Found {len(teps)} TREAT-AD community records")
    return teps


def categorize_title(title: str) -> str:
    """Classify a record as a TEP report or a supporting component by title.

    Records whose title mentions "component" or "resource" are supporting
    materials, everything else (including "package"/"report") is treated as a
    main TEP report.

    Arguments:
        title (str): The record title

    Returns:
        str: "component" for supporting materials, otherwise "report"
    """
    title_lower = title.lower()
    if "component" in title_lower or "resource" in title_lower:
        return "component"
    return "report"


def _validate_record(index: int, record: Dict[str, Any]) -> List[str]:
    """Validate a single TEP record and return (and log) any error messages.

    Checks performed:
      1) The record contains all REQUIRED_RECORD_FIELDS
      2) Metric fields are non-negative integers
      3) title, date and link fields have non-empty values

    Arguments:
        index (int): Position of the record in the batch (for error messages)
        record (Dict[str, Any]): A single processed record

    Returns:
        List[str]: Validation error messages (empty if the record is valid)
    """
    errors: List[str] = []

    missing = [f for f in REQUIRED_RECORD_FIELDS if f not in record]
    if missing:
        errors.append(f"Validation failed: record {index} missing fields: {missing}")

    for field in METRIC_FIELDS:
        value = record.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            errors.append(
                f"Validation failed: record {index} field '{field}' is not a "
                f"non-negative integer (got {value!r})."
            )

    for field in ["title", "date", "link"]:
        if not record.get(field):
            errors.append(f"Validation failed: record {index} has empty '{field}'.")

    for msg in errors:
        logger.error(msg)
    return errors


def validate_records(records: List[Dict[str, Any]]) -> None:
    """Validate the pulled TEP metrics for schema stability.

    Logs all validation issues and only raises at the end if at least one check
    fails, so the Airflow logs contain maximum debug info while still failing the
    run (and triggering the failure alert) on bad output.

    Checks performed:
      1) At least one record was returned.
      2) Each record passes the per-record checks in ``_validate_record``.

    Arguments:
        records (List[Dict[str, Any]]): Processed records from fetch_tep_records

    Raises:
        ValueError: If one or more validation checks fail with the details of
            all failures
    """
    errors: List[str] = []

    if not records:
        msg = "Validation failed: Zenodo returned zero TREAT-AD TEP records."
        logger.error(msg)
        errors.append(msg)

    for i, record in enumerate(records):
        errors.extend(_validate_record(i, record))

    if errors:
        raise ValueError(
            "Zenodo TEP metrics validation failed:\n- " + "\n- ".join(errors)
        )

    logger.info(f"Validation passed for {len(records)} records.")


class CsvReport(NamedTuple):
    """A single CSV report to write.

    Attributes:
        path (str): Output path for the CSV file
        records (List[Dict[str, Any]]): Records belonging to this report
    """

    path: str
    records: List[Dict[str, Any]]


def build_reports(records: List[Dict[str, Any]], filepath: str) -> Dict[str, Any]:
    """Build CSV reports of TEP metrics and save them locally.

    Writes two CSV files split by record category ("TEP Reports" and
    "TEP Components"), each with a header row, rows of data, a spacer row, and a
    totals row.

    The two CSV paths are derived from filepath by stripping its extension and
    appending _TEP_Reports.csv / _TEP_Components.csv.

    Arguments:
        records (List[Dict[str, Any]]): Validated TEP records
        filepath (str): Base path used to derive the two CSV file paths

    Returns:
        Dict[str, Any]: Counts of report/component records and the list of
            written CSV file paths (under the "paths" key)
    """
    main_teps = [r for r in records if r["category"] == "report"]
    supporting = [r for r in records if r["category"] == "component"]

    base = os.path.splitext(filepath)[0]
    outputs = [
        CsvReport(path=f"{base}_TEP_Reports.csv", records=main_teps),
        CsvReport(path=f"{base}_TEP_Components.csv", records=supporting),
    ]

    for output in outputs:
        with open(output.path, "w", newline="") as f:
            writer = csv.writer(f)

            writer.writerow(EXCEL_HEADERS)

            for record in output.records:
                writer.writerow(
                    [
                        record["title"],
                        record["date"],
                        record["views"],
                        record["unique_views"],
                        record["downloads"],
                        record["unique_downloads"],
                        record["link"],
                    ]
                )

            writer.writerow([])  # spacer row
            writer.writerow(
                [
                    "TOTALS",
                    "",
                    sum(r["views"] for r in output.records),
                    sum(r["unique_views"] for r in output.records),
                    sum(r["downloads"] for r in output.records),
                    sum(r["unique_downloads"] for r in output.records),
                    "",
                ]
            )

    logger.info(
        f"Wrote CSV reports to {outputs[0].path} and {outputs[1].path} "
        f"({len(main_teps)} reports, {len(supporting)} components)"
    )
    return {
        "report_records": len(main_teps),
        "component_records": len(supporting),
        "paths": [output.path for output in outputs],
    }


def export_reports_to_synapse(
    records: List[Dict[str, Any]],
    run_date: str,
    synapse_conn_id: str,
    folder_id: str,
) -> List[Dict[str, str]]:
    """Build the CSV reports and upload them to a Synapse folder.

    Local CSV files are always removed afterwards, even if an upload fails.

    Arguments:
        records (List[Dict[str, Any]]): Validated TEP records
        run_date (str): YYYYMMDD date string used in the report filenames
        synapse_conn_id (str): Synapse connection id
        folder_id (str): Synapse folder/project to upload the reports to

    Returns:
        List[Dict[str, str]]: Uploaded file info ("id" and "name") for each CSV
    """
    base_filepath = os.path.join(
        os.getcwd(), f"TREATAD_Target_Enabling_Metrics_{run_date}"
    )

    csv_paths = build_reports(records, base_filepath)["paths"]
    uploaded_files: List[Dict[str, str]] = []
    try:
        client = SynapseHook(synapse_conn_id).client
        for csv_path in csv_paths:
            filename = os.path.basename(csv_path)
            uploaded = File(path=csv_path, parent_id=folder_id).store(
                synapse_client=client
            )
            logger.info(f"Uploaded {filename} to Synapse as {uploaded.id}")
            uploaded_files.append({"id": uploaded.id, "name": filename})
    finally:
        for csv_path in csv_paths:
            if os.path.exists(csv_path):
                os.remove(csv_path)

    return uploaded_files


@dag(
    on_failure_callback=synapse_failure_callback(
        message=(
            "This may indicate a Zenodo API schema change or a transient API "
            "issue. Please review the task logs."
        )
    ),
    **dag_config,
)
def zenodo_tep_metrics_dag() -> AirflowDAG:
    """Pull TREAT-AD TEP metrics from Zenodo, validate, export to Synapse, notify."""

    @task
    def fetch_metrics(**context) -> List[Dict[str, Any]]:
        """Fetch TREAT-AD TEP records from the Zenodo API.

        Returns:
            List[Dict[str, Any]]: Processed TEP records
        """
        api_token = Variable.get("ZENODO_API_TOKEN")
        return fetch_tep_records(api_token)

    @task
    def validate_metrics(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate the pulled metrics for schema stability.

        Arguments:
            records (List[Dict[str, Any]]): Records from fetch_metrics

        Returns:
            List[Dict[str, Any]]: The validated records
        """
        validate_records(records)
        return records

    @task
    def export_to_synapse(
        records: List[Dict[str, Any]], **context
    ) -> List[Dict[str, str]]:
        """Build the CSV reports and upload them to Synapse.

        Arguments:
            records (List[Dict[str, Any]]): Validated TEP records
            **context: Airflow task context containing DAG parameters

        Returns:
            List[Dict[str, str]]: Uploaded file info (id and name) for each CSV
        """
        return export_reports_to_synapse(
            records,
            run_date=context["ds_nodash"],  # YYYYMMDD of the logical run date
            synapse_conn_id=context["params"]["synapse_conn_id"],
            folder_id=context["params"]["synapse_export_folder"],
        )

    @task
    def notify_collaborators(uploaded_files: List[Dict[str, str]], **context) -> None:
        """Notify collaborator(s) that the monthly reports are available.

        Arguments:
            uploaded_files (List[Dict[str, str]]): Uploaded file info from
                export_to_synapse.
            **context: Airflow task context containing DAG parameters
        """
        links = "\n".join(
            f"- {f['name']}: https://www.synapse.org/Synapse:{f['id']}"
            for f in uploaded_files
        )
        subject = "New TREAT-AD Target Enabling Metrics reports available"
        body = (
            "New monthly TREAT-AD target enabling metrics reports have been "
            f"uploaded to Synapse:\n\n{links}"
        )
        send_synapse_message(
            conn_id=context["params"]["synapse_conn_id"],
            usernames=context["params"]["collaborator_user_list"].split(","),
            subject=subject,
            body=body,
        )

    records = fetch_metrics()
    validated = validate_metrics(records)
    file_info = export_to_synapse(validated)
    notify_collaborators(file_info)


dag = zenodo_tep_metrics_dag()

if __name__ == "__main__":
    dag.test()

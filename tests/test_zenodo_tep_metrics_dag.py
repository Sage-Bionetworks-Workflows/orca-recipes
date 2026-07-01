import csv

import pytest

from dags import zenodo_tep_metrics_dag as dag_module

VALID_RECORD = {
    "title": "Target Enabling Package Report",
    "date": "2025-01-01",
    "views": 10,
    "unique_views": 7,
    "downloads": 4,
    "unique_downloads": 3,
    "link": "https://zenodo.org/records/123",
    "category": "report",
}


def test_categorize_title_report():
    assert dag_module.categorize_title("Target Enabling Package Report") == "report"


def test_categorize_title_component():
    assert dag_module.categorize_title("Target Enabling Component Dataset") == "component"
    assert dag_module.categorize_title("Supporting Resource File") == "component"


def test_validate_records_accepts_valid_record():
    dag_module.validate_records([VALID_RECORD])


@pytest.mark.parametrize(
    "bad_record, expected_message",
    [
        ({**VALID_RECORD, "views": -1}, "non-negative integer"),
        ({**VALID_RECORD, "views": True}, "non-negative integer"),
        ({**VALID_RECORD, "title": ""}, "empty 'title'"),
    ],
    ids = ["negative views", "boolean views", "empty title"]
)
def test_validate_records_rejects_invalid_record(bad_record, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        dag_module.validate_records([bad_record])


def test_validate_records_rejects_missing_field():
    bad_record = VALID_RECORD.copy()
    bad_record.pop("link")

    with pytest.raises(ValueError, match="missing fields"):
        dag_module.validate_records([bad_record])


def test_validate_records_rejects_empty_records():
    with pytest.raises(ValueError, match="zero TREAT-AD TEP records"):
        dag_module.validate_records([])


def _read_csv(path):
    """Read a CSV file into a list of rows (each row a list of string cells)."""
    with open(path, newline="") as f:
        return list(csv.reader(f))


def test_build_reports_creates_expected_csvs_and_totals(tmp_path):
    records = [
        VALID_RECORD,
        {
            **VALID_RECORD,
            "title": "Supporting Component",
            "views": 5,
            "unique_views": 4,
            "downloads": 2,
            "unique_downloads": 1,
            "category": "component",
        },
    ]

    output = tmp_path / "metrics.xlsx"

    result = dag_module.build_reports(records, str(output))

    reports_path = str(tmp_path / "metrics_TEP_Reports.csv")
    components_path = str(tmp_path / "metrics_TEP_Components.csv")

    assert result["report_records"] == 1
    assert result["component_records"] == 1
    assert result["paths"] == [reports_path, components_path]

    # CSV cells are read back as strings.
    reports = _read_csv(reports_path)
    assert reports[0] == dag_module.EXCEL_HEADERS  # header row
    assert reports[1][0] == "Target Enabling Package Report"  # data row
    assert reports[-1] == ["TOTALS", "", "10", "7", "4", "3", ""]  # totals row

    components = _read_csv(components_path)
    assert components[1][0] == "Supporting Component"
    assert components[-1] == ["TOTALS", "", "5", "4", "2", "1", ""]


class MockZenodoResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


def test_fetch_tep_records_filters_dedupes_and_categorizes(monkeypatch):
    payload = {
        "hits": {
            "total": 2,
            "hits": [
                {
                    "id": 123,
                    "metadata": {
                        "title": "Target Enabling Package Report",
                        "publication_date": "2025-01-01",
                        "communities": [{"id": "treatad"}],
                    },
                    "stats": {
                        "views": 10,
                        "unique_views": 7,
                        "downloads": 4,
                        "unique_downloads": 3,
                    },
                },
                {
                    "id": 456,
                    "metadata": {
                        "title": "Other Community Report",
                        "publication_date": "2025-01-02",
                        "communities": [{"id": "other"}],
                    },
                    "stats": {},
                },
            ],
        }
    }

    def mock_get(*args, **kwargs):
        return MockZenodoResponse(payload)

    monkeypatch.setattr(dag_module.requests, "get", mock_get)

    records = dag_module.fetch_tep_records(
        api_token="fake-token",
        search_terms=["target enabling"],
        community_id="treatad",
    )

    assert records == [
        {
            "title": "Target Enabling Package Report",
            "date": "2025-01-01",
            "views": 10,
            "unique_views": 7,
            "downloads": 4,
            "unique_downloads": 3,
            "link": "https://zenodo.org/records/123",
            "category": "report",
        }
    ]


def test_fetch_tep_records_paginates(monkeypatch):
    calls = []

    def make_record(record_id):
        return {
            "id": record_id,
            "metadata": {
                "title": f"Report {record_id}",
                "publication_date": "2025-01-01",
                "communities": [{"id": "treatad"}],
            },
            "stats": {},
        }

    page_1 = {
        "hits": {
            "total": 101,
            "hits": [make_record(i) for i in range(100)],
        }
    }

    page_2 = {
        "hits": {
            "total": 101,
            "hits": [make_record(100)],
        }
    }

    def mock_get(url, params, headers, timeout):
        calls.append(params["page"])
        return MockZenodoResponse(page_1 if params["page"] == 1 else page_2)

    monkeypatch.setattr(dag_module.requests, "get", mock_get)

    records = dag_module.fetch_tep_records(
        api_token="fake-token",
        search_terms=["target enabling"],
        community_id="treatad",
    )

    assert calls == [1, 2]
    assert len(records) == 101
    
    
def test_send_synapse_message(monkeypatch):
    sent = {}

    class MockClient:
        def getUserProfile(self, username):
            return {"ownerId": f"owner-{username}"}

        def sendMessage(self, owner_ids, subject, body):
            sent["owner_ids"] = owner_ids
            sent["subject"] = subject
            sent["body"] = body

    class MockSynapseHook:
        def __init__(self, conn_id):
            self.conn_id = conn_id
            self.client = MockClient()

    monkeypatch.setattr(dag_module, "SynapseHook", MockSynapseHook)

    dag_module._send_synapse_message(
        conn_id="synapse_conn",
        usernames=["alice", " bob ", ""],
        subject="Test subject",
        body="Test body",
    )

    assert sent == {
        "owner_ids": ["owner-alice", "owner-bob"],
        "subject": "Test subject",
        "body": "Test body",
    }


def test_alert_on_failure_calls_send_synapse_message(monkeypatch):
    sent = {}

    def mock_send_synapse_message(conn_id, usernames, subject, body):
        sent["conn_id"] = conn_id
        sent["usernames"] = usernames
        sent["subject"] = subject
        sent["body"] = body

    class MockTaskInstance:
        task_id = "fetch_metrics"
        dag_id = "zenodo_tep_metrics_dag"
        run_id = "manual__2025-01-01"
        log_url = "http://airflow/log"

    monkeypatch.setattr(
        dag_module,
        "_send_synapse_message",
        mock_send_synapse_message,
    )

    dag_module.alert_on_failure(
        {
            "params": {
                "synapse_conn_id": "synapse_conn",
                "dev_user_list": "alice,bob",
            },
            "task_instance": MockTaskInstance(),
            "exception": RuntimeError("boom"),
            "logical_date": "2025-01-01",
        }
    )

    assert sent["conn_id"] == "synapse_conn"
    assert sent["usernames"] == ["alice", "bob"]
    assert "Zenodo TEP Metrics DAG failure" in sent["subject"]
    assert "fetch_metrics" in sent["body"]
    assert "boom" in sent["body"]
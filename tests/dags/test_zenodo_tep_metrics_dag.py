import csv
import os

import pytest
from airflow.models import DagBag

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
    assert (
        dag_module.categorize_title("Target Enabling Component Dataset") == "component"
    )
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
    ids=["negative views", "boolean views", "empty title"],
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


def test_validate_record_returns_no_errors_for_valid_record():
    assert dag_module._validate_record(0, VALID_RECORD) == []


@pytest.mark.parametrize(
    "bad_record, expected_message",
    [
        ({**VALID_RECORD, "downloads": -5}, "non-negative integer"),
        ({**VALID_RECORD, "downloads": True}, "non-negative integer"),
        ({**VALID_RECORD, "downloads": "4"}, "non-negative integer"),
        ({**VALID_RECORD, "date": ""}, "empty 'date'"),
    ],
    ids=["negative", "boolean", "non-int", "empty date"],
)
def test_validate_record_flags_bad_field(bad_record, expected_message):
    errors = dag_module._validate_record(2, bad_record)
    assert len(errors) == 1
    assert expected_message in errors[0]
    assert "record 2" in errors[0]  # the record index is included in the message


def test_validate_record_reports_missing_field():
    bad_record = VALID_RECORD.copy()
    bad_record.pop("link")

    joined = "\n".join(dag_module._validate_record(0, bad_record))
    assert "missing fields" in joined
    assert "link" in joined


def test_validate_record_aggregates_multiple_errors():
    bad_record = {**VALID_RECORD, "views": -1, "title": ""}

    errors = dag_module._validate_record(3, bad_record)
    joined = "\n".join(errors)
    assert len(errors) == 2
    assert "non-negative integer" in joined
    assert "empty 'title'" in joined


def _read_csv(path):
    """Read a CSV file into a list of rows (each row a list of string cells)."""
    with open(path, newline="") as f:
        return list(csv.reader(f))


def test_get_report_outputs_splits_records_and_builds_paths(tmp_path):
    records = [
        VALID_RECORD,
        {
            **VALID_RECORD,
            "title": "Supporting Component",
            "category": "component",
        },
    ]

    outputs = dag_module.get_report_outputs(
        records,
        str(tmp_path / "metrics.xlsx"),
    )

    assert outputs == [
        dag_module.CsvReport(
            path=str(tmp_path / "metrics_TEP_Reports.csv"),
            records=[records[0]],
        ),
        dag_module.CsvReport(
            path=str(tmp_path / "metrics_TEP_Components.csv"),
            records=[records[1]],
        ),
    ]


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

    outputs = dag_module.get_report_outputs(
        records,
        str(tmp_path / "metrics.xlsx"),
    )

    result = dag_module.build_reports(outputs)

    reports_path = str(tmp_path / "metrics_TEP_Reports.csv")
    components_path = str(tmp_path / "metrics_TEP_Components.csv")

    assert result == {
        "report_records": 1,
        "component_records": 1,
        "paths": [reports_path, components_path],
    }

    # CSV cells are read back as strings.
    reports = _read_csv(reports_path)
    assert reports[0] == dag_module.CSV_HEADERS
    assert reports[1][0] == "Target Enabling Package Report"
    assert reports[-1] == ["TOTALS", "", "10", "7", "4", "3", ""]

    components = _read_csv(components_path)
    assert components[0] == dag_module.CSV_HEADERS
    assert components[1][0] == "Supporting Component"
    assert components[-1] == ["TOTALS", "", "5", "4", "2", "1", ""]


def test_export_reports_to_synapse_cleans_up_when_second_upload_fails(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(dag_module.os, "getcwd", lambda: str(tmp_path))

    uploaded_paths = []

    class MockUploaded:
        id = "syn-first-upload"

    class MockFile:
        def __init__(self, path, parent_id, version_comment=None):
            self.path = path
            self.parent_id = parent_id
            self.version_comment = version_comment

        def store(self, synapse_client=None):
            uploaded_paths.append(self.path)

            if len(uploaded_paths) == 2:
                raise RuntimeError("Second Synapse upload failed")

            return MockUploaded()

    class MockSynapseHook:
        def __init__(self, conn_id):
            self.client = object()

    monkeypatch.setattr(dag_module, "File", MockFile)
    monkeypatch.setattr(dag_module, "SynapseHook", MockSynapseHook)

    with pytest.raises(RuntimeError, match="Second Synapse upload failed"):
        dag_module.export_reports_to_synapse(
            [VALID_RECORD],
            synapse_conn_id="conn",
            folder_id="syn123",
        )

    assert len(uploaded_paths) == 2
    assert not list(tmp_path.glob("*.csv"))


class MockZenodoResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        """Empty to simulate a successful HTTP response
        """
        pass

    def json(self):
        return self.payload


def test_fetch_tep_records_processes_and_categorizes(monkeypatch):
    payload = {
        "hits": {
            "total": 2,
            "hits": [
                {
                    "id": 123,
                    "metadata": {
                        "title": "Target Enabling Package Report",
                        "publication_date": "2025-01-01",
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
                        "title": "Supporting Component Dataset",
                        "publication_date": "2025-01-02",
                    },
                    "stats": {},  # missing metrics should default to 0
                },
            ],
        }
    }

    def mock_get(*args, **kwargs):
        return MockZenodoResponse(payload)

    monkeypatch.setattr(dag_module.requests, "get", mock_get)

    records = dag_module.fetch_tep_records(
        api_token="fake-token",
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
        },
        {
            "title": "Supporting Component Dataset",
            "date": "2025-01-02",
            "views": 0,
            "unique_views": 0,
            "downloads": 0,
            "unique_downloads": 0,
            "link": "https://zenodo.org/records/456",
            "category": "component",
        },
    ]


def test_fetch_tep_records_paginates(monkeypatch):
    calls = []

    def make_record(record_id):
        return {
            "id": record_id,
            "metadata": {
                "title": f"Report {record_id}",
                "publication_date": "2025-01-01",
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
        community_id="treatad",
    )

    assert calls == [1, 2]
    assert len(records) == 101


def test_export_reports_to_synapse(monkeypatch, tmp_path):
    # Write the CSVs into tmp_path instead of the real working directory.
    monkeypatch.setattr(dag_module.os, "getcwd", lambda: str(tmp_path))

    version_comments = []

    class MockUploaded:
        def __init__(self, id):
            self.id = id

    class MockFile:
        def __init__(self, path, parent_id, version_comment=None):
            self.path = path
            self.parent_id = parent_id
            self.version_comment = version_comment

        def store(self, synapse_client=None):
            version_comments.append(self.version_comment)
            return MockUploaded(id="syn_" + os.path.basename(self.path))

    class MockSynapseHook:
        def __init__(self, conn_id):
            self.client = object()

    monkeypatch.setattr(dag_module, "File", MockFile)
    monkeypatch.setattr(dag_module, "SynapseHook", MockSynapseHook)

    result = dag_module.export_reports_to_synapse(
        [VALID_RECORD],
        synapse_conn_id="conn",
        folder_id="syn123",
    )

    # One entry per uploaded CSV, with the derived (date-free) filename and id.
    assert result == [
        {
            "id": "syn_TREATAD_Target_Enabling_Metrics_TEP_Reports.csv",
            "name": "TREATAD_Target_Enabling_Metrics_TEP_Reports.csv",
        },
        {
            "id": "syn_TREATAD_Target_Enabling_Metrics_TEP_Components.csv",
            "name": "TREATAD_Target_Enabling_Metrics_TEP_Components.csv",
        },
    ]
    # Every upload carries the provenance version comment.
    assert version_comments == [dag_module.SYNAPSE_VERSION_COMMENT] * 2
    # Local CSV files are cleaned up after upload.
    assert not list(tmp_path.glob("*.csv"))


def test_export_reports_to_synapse_cleans_up_on_upload_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(dag_module.os, "getcwd", lambda: str(tmp_path))

    class MockFailingFile:
        def __init__(self, path, parent_id, version_comment=None):
            """The constructor accepts the same arguments as the real File class but
            intentionally ignores them because relevant test is only verifying that
            export_reports_to_synapse() cleans up temporary CSVs when store()
            raises an exception.
            """
            pass

        def store(self, synapse_client=None):
            raise RuntimeError("Synapse upload failed")

    class MockSynapseHook:
        def __init__(self, conn_id):
            self.client = object()

    monkeypatch.setattr(dag_module, "File", MockFailingFile)
    monkeypatch.setattr(dag_module, "SynapseHook", MockSynapseHook)

    with pytest.raises(RuntimeError, match="Synapse upload failed"):
        dag_module.export_reports_to_synapse(
            [VALID_RECORD],
            synapse_conn_id="conn",
            folder_id="syn123",
        )
    # Even when the upload raises, the local CSVs are removed.
    assert not list(tmp_path.glob("*.csv"))


# DAG specific tests
@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags/zenodo_tep_metrics_dag.py", include_examples=False)


def test_dag_loads_with_no_issues(dagbag):
    assert dagbag.import_errors == {}
    dag = dagbag.dags["zenodo_tep_metrics_dag"]
    assert dag is not None


def test_dag_structure_is_correct(dagbag):
    # Read from the parsed .dags dict (get_dag() hits the Airflow metadata DB,
    # which isn't available in CI).
    assert "zenodo_tep_metrics_dag" in dagbag.dags, "zenodo_tep_metrics_dag failed to load"
    dag = dagbag.dags["zenodo_tep_metrics_dag"]
    assert {task.task_id for task in dag.tasks} == {
        "fetch_metrics",
        "validate_metrics",
        "export_to_synapse",
        "notify_collaborators",
    }
    # Verify the linear dependency chain fetch -> validate -> export -> notify.
    assert dag.get_task("fetch_metrics").downstream_task_ids == {"validate_metrics"}
    assert dag.get_task("validate_metrics").downstream_task_ids == {"export_to_synapse"}
    assert dag.get_task("export_to_synapse").downstream_task_ids == {
        "notify_collaborators"
    }

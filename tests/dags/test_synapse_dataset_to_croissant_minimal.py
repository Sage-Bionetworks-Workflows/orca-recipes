"""Tests for dags/synapse_dataset_to_croissant_minimal.py."""

import importlib
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from airflow.sdk import Variable
from synapseclient.core.exceptions import SynapseHTTPError


@pytest.fixture(scope="module")
def dag_module() -> ModuleType:
    """Imports the DAG module with the parse-time SigNoz Variable lookup mocked.

    The module reads SIGNOZ_INGESTION_KEY at import time to set up OpenTelemetry
    (see KNOWN_PARSE_TIME_DEPENDENCIES in test_all_dags.py), so it can't be
    imported in tests without this.
    """
    with patch.object(Variable, "get", return_value="fake-signoz-key"):
        return importlib.import_module("dags.synapse_dataset_to_croissant_minimal")


@pytest.fixture
def fake_context() -> dict[str, dict[str, Any]]:
    """Mimics the Airflow task context, which exposes DAG run params under "params"."""
    return {
        "params": {
            "synapse_conn_id": "synapse_default",
            "bucket_name": "test-bucket",
            "synapse_table_for_croissant_links": "syn123",
            "push_results_to_s3": False,
            "push_links_to_synapse": False,
            "collaborator_user_list": "alice,bob",
            "collaborator_team_list": "3622772",
        }
    }


@pytest.fixture
def catalog_row() -> pd.DataFrame:
    """A single-row Synapse data catalog query result."""
    return pd.DataFrame(
        {
            "id": ["syn999"],
            "name": ["My Dataset"],
            "description": ["desc"],
            "contributors": ["Sage"],
            "license": ["CC-BY"],
        }
    )


def test_dev_alert_recipients_target_testing_team(dag_module: ModuleType) -> None:
    """Tests that dev (the default environment in tests) alerts the testing team only."""
    params = dag_module.dag_params
    assert params["collaborator_user_list"].value is None
    assert params["collaborator_team_list"].value == "3622772"
    assert params["dev_user_list"].value is None
    assert params["dev_team_list"].value == "3622772"


def test_lack_of_read_access_handles_unset_recipient_list(
    dag_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    fake_context: dict[str, dict[str, Any]],
    catalog_row: pd.DataFrame,
) -> None:
    """Tests that a None (unset) recipient list is treated as empty, not an error."""
    fake_context["params"]["collaborator_user_list"] = None
    mock_hook = MagicMock()
    mock_hook.return_value.client.get.side_effect = SynapseHTTPError(
        "403 Client Error: You lack READ access to the requested entity."
    )
    mock_send = MagicMock()
    monkeypatch.setattr(dag_module, "SynapseHook", mock_hook)
    monkeypatch.setattr(dag_module, "query", MagicMock(return_value=catalog_row))
    monkeypatch.setattr(dag_module, "send_synapse_message", mock_send)

    task = dag_module.dag.get_task("create_and_save_jsonld").python_callable
    task(**fake_context)

    kwargs = mock_send.call_args.kwargs
    assert kwargs["usernames"] == [""]
    assert kwargs["team_ids"] == ["3622772"]


def test_dag_has_failure_callback(dag_module: ModuleType) -> None:
    assert dag_module.dag.on_failure_callback


def test_task_wiring(dag_module: ModuleType) -> None:
    """Tests that both cleanup tasks run after the JSON-LD files are created."""
    dag = dag_module.dag
    assert dag.get_task("create_and_save_jsonld").downstream_task_ids == {
        "delete_non_current_croissant_file_in_s3",
        "delete_non_current_croissant_file_in_synapse",
    }


def test_lack_of_read_access_alerts_collaborators_and_skips_dataset(
    dag_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    fake_context: dict[str, dict[str, Any]],
    catalog_row: pd.DataFrame,
) -> None:
    """Tests that a dataset without READ access emails the collaborator users and
    teams and is not pushed anywhere."""
    mock_hook = MagicMock()
    mock_hook.return_value.client.get.side_effect = SynapseHTTPError(
        "403 Client Error: You lack READ access to the requested entity."
    )
    mock_send = MagicMock()
    mock_push_to_s3 = MagicMock()
    monkeypatch.setattr(dag_module, "SynapseHook", mock_hook)
    monkeypatch.setattr(dag_module, "query", MagicMock(return_value=catalog_row))
    monkeypatch.setattr(dag_module, "send_synapse_message", mock_send)
    monkeypatch.setattr(dag_module, "execute_push_to_s3", mock_push_to_s3)

    task = dag_module.dag.get_task("create_and_save_jsonld").python_callable
    result = task(**fake_context)

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["conn_id"] == "synapse_default"
    assert kwargs["usernames"] == ["alice", "bob"]
    assert kwargs["team_ids"] == ["3622772"]
    assert "syn999" in kwargs["body"]
    mock_push_to_s3.assert_not_called()
    # Still listed as current so the cleanup tasks don't delete its existing files
    assert result == [{"dataset_id": "syn999", "dataset_name": "My Dataset"}]


def test_other_synapse_errors_are_raised_without_alert(
    dag_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    fake_context: dict[str, dict[str, Any]],
    catalog_row: pd.DataFrame,
) -> None:
    mock_hook = MagicMock()
    mock_hook.return_value.client.get.side_effect = SynapseHTTPError(
        "500 Server Error: Internal Server Error"
    )
    mock_send = MagicMock()
    monkeypatch.setattr(dag_module, "SynapseHook", mock_hook)
    monkeypatch.setattr(dag_module, "query", MagicMock(return_value=catalog_row))
    monkeypatch.setattr(dag_module, "send_synapse_message", mock_send)

    task = dag_module.dag.get_task("create_and_save_jsonld").python_callable
    with pytest.raises(SynapseHTTPError, match="500 Server Error"):
        task(**fake_context)

    mock_send.assert_not_called()

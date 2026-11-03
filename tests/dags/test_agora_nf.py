"""Tests for dags/agora_nf.py."""

from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dags.agora_nf import dag
from src import synapse_alerts
from src.nextflow_tower_hook import (
    NextflowTowerHook,
    Workflow,
    WorkflowState,
    WorkflowStatus,
)


RUN_ID = "tw-run"
RUN_NAME = "test_run"
DATASET = "test_dataset1, test_dataset2"
# A dataset Tower may have recorded for the run, which must never reach the message
TOWER_DATASET = "tower-recorded-workflow-dataset"

@pytest.fixture
def mock_tower(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replaces the NextflowTowerHook methods the DAG calls with a fresh mock per test.

    monkeypatch undoes the attribute changes at teardown, and building the mock inside
    the fixture keeps call history from leaking between tests. A MagicMock attribute is
    not a descriptor, so calls through a hook instance do not pass `self`, and
    call_args holds exactly the arguments the DAG task passed.
    """
    mock = MagicMock()
    mock.launch_workflow.return_value = RUN_ID
    monkeypatch.setattr(NextflowTowerHook, "launch_workflow", mock.launch_workflow)
    monkeypatch.setattr(NextflowTowerHook, "get_workflow", mock.get_workflow)
    return mock


@pytest.fixture
def fake_context() -> dict[str, dict[str, Any]]:
    """Mimics the Airflow task context, which exposes DAG run params under "params"."""
    return {
        "params": {
            "tower_conn_id": "nextflow_default",
            "synapse_conn_id": "synapse_default",
            "tower_compute_env_type": "agora-project-ondemand-v13-test",
            "tower_run_name": RUN_NAME,
            "pipeline": "Sage-Bionetworks-Workflows/nf-agora",
            "revision": "test_branch_name",
            "profile": "test_profile",
            "work_dir": "s3://agora-project-tower-test/work",
            "default_memory_gb": 10,
            "large_memory_gb": 20,
            "large_memory_datasets": "dataset_a,dataset_b",
            "dataset": DATASET,
            "slack_channel": "test-agora-nextflow",
            "dpe_slack_bot_conn": "dpe_slack_bot_test",
            "synapse_team_id": "1234567",
        }
    }


def test_launch_agora_on_tower(
    fake_context: dict[str, dict[str, Any]], mock_tower: MagicMock
) -> None:
    """Tests the launch_agora_on_tower task with input parameters."""

    # Task functions are wrapped by the @task decorator; python_callable is the underlying function
    raw_python_function = dag.get_task("launch_agora_on_tower").python_callable
    run_id = raw_python_function(**fake_context)
    mock_tower.launch_workflow.assert_called_once()
    assert run_id == RUN_ID

    # Validate the full LaunchInfo payload and the remaining launch_workflow call args
    called_args, called_kwargs = mock_tower.launch_workflow.call_args
    launched_info_object, compute_env = called_args

    assert launched_info_object.run_name == RUN_NAME
    assert launched_info_object.pipeline == "Sage-Bionetworks-Workflows/nf-agora"
    assert launched_info_object.revision == "test_branch_name"
    assert launched_info_object.work_dir == "s3://agora-project-tower-test/work"
    assert launched_info_object.profiles == ["test_profile"]
    assert launched_info_object.workspace_secrets == ["SYNAPSE_AUTH_TOKEN"]
    assert launched_info_object.params == {
        "default_memory_gb": 10,
        "large_memory_gb": 20,
        "large_memory_datasets": "dataset_a,dataset_b",
        "dataset": DATASET,
    }
    assert compute_env == "agora-project-ondemand-v13-test"
    assert called_kwargs == {"ignore_previous_runs": True}

@pytest.mark.parametrize(
    "state",
    [WorkflowState.SUCCEEDED, WorkflowState.FAILED, WorkflowState.CANCELLED, WorkflowState.UNKNOWN, WorkflowState.RUNNING],
)
def test_monitor_agora_workflow_state(
    fake_context: dict[str, dict[str, Any]], mock_tower: MagicMock, state: WorkflowState
) -> None:
    """Tests that monitor_nf_agora_workflow reports done only for terminal workflow states."""
    mock_tower.get_workflow.return_value.status = WorkflowStatus(state=state)

    raw_python_function = dag.get_task("monitor_nf_agora_workflow").python_callable
    workflow_state = raw_python_function(run_id=RUN_ID, **fake_context)

    # RUNNING is the only non-terminal state in this list, so it's the only one expecting False
    if state == WorkflowState.RUNNING:
        assert workflow_state is False
    else:
        assert workflow_state is True
    mock_tower.get_workflow.assert_called_once_with(RUN_ID)


def make_workflow(state: WorkflowState, **overrides: Any) -> Workflow:
    """Build a real Workflow from an API-shaped JSON payload, as Tower returns it.

    Both submit and complete are set, so generate_message can compute a duration
    instead of falling back to "unknown".
    """
    payload = {
        "id": RUN_ID,
        "runName": RUN_NAME,
        "status": state.value,
        "submit": "2026-01-01T00:00:00Z",
        "complete": "2026-01-01T01:30:00Z",
        **overrides,
    }
    return Workflow.from_json(payload)


@pytest.mark.parametrize(
    "state",
    [WorkflowState.SUCCEEDED, WorkflowState.FAILED, WorkflowState.CANCELLED, WorkflowState.UNKNOWN],
)
@pytest.mark.parametrize(
    "dag_params, expected_dataset",
    [
        pytest.param({"dataset": DATASET}, DATASET, id="dataset-set"),
        pytest.param({"dataset": None}, "all datasets", id="dataset-null"),
        pytest.param({"dataset": ""}, "all datasets", id="dataset-empty"),
        pytest.param({}, "all datasets", id="dataset-absent"),
    ],
)
@pytest.mark.parametrize(
    "tower_params",
    [
        # Tower returns params as null when a run fails before Nextflow starts
        pytest.param(None, id="tower-params-null"),
        # Once Nextflow has started, Tower reports the params the run was given
        pytest.param({"dataset": TOWER_DATASET}, id="tower-params-set"),
    ],
)
def test_generate_message(
    fake_context: dict[str, dict[str, Any]],
    mock_tower: MagicMock,
    state: WorkflowState,
    dag_params: dict[str, Any],
    expected_dataset: str,
    tower_params: dict[str, Any] | None,
) -> None:
    """Tests that generate_message reports the run name, ID, state, duration and dataset.

    The dataset comes from the DAG params and falls back to "all datasets" when unset.
    It never comes from the params Tower recorded for the run (tower_params), which are
    null if the run failed before Nextflow started. The message is therefore the same
    whether those params are null or set, for every terminal state.
    """
    fake_context["params"].pop("dataset")
    fake_context["params"].update(dag_params)
    mock_tower.get_workflow.return_value = make_workflow(state, params=tower_params)

    raw_python_function = dag.get_task("generate_message").python_callable
    message = raw_python_function(run_id=RUN_ID, **fake_context)

    assert f"Tower workflow (Name: {RUN_NAME}, Id: {RUN_ID}) has completed with state: {state.value}" in message
    assert f"Duration (submission to completion): {timedelta(hours=1, minutes=30)}" in message
    assert f"Dataset: {expected_dataset}" in message
    assert TOWER_DATASET not in message


def test_post_slack_messages(fake_context: dict[str, dict[str, Any]]) -> None:
    """Tests that post_slack_messages returns True when the Slack API call succeeds."""
    with patch("dags.agora_nf.SlackHook") as mock_slack_hook:
        mock_client = MagicMock()
        mock_slack_hook.return_value.client = mock_client
        mock_client.chat_postMessage.return_value = {"ok": True}

        raw_python_function = dag.get_task("post_slack_messages").python_callable
        result = raw_python_function(message="Test message", **fake_context)

        assert result is True
        mock_slack_hook.assert_called_once_with(
            slack_conn_id=fake_context["params"]["dpe_slack_bot_conn"]
        )
        mock_client.chat_postMessage.assert_called_once_with(
            channel=fake_context["params"]["slack_channel"], text="Test message"
        )


def test_post_email_messages(fake_context: dict[str, dict[str, Any]]) -> None:
    """Tests that post_email_messages returns True when the Synapse API call succeeds."""
    with patch("dags.agora_nf.SynapseHook") as mock_synapse_hook:
        mock_synapse_client = MagicMock()
        mock_synapse_hook.return_value.client = mock_synapse_client

        raw_python_function = dag.get_task("post_email_messages").python_callable
        result = raw_python_function(message="Test message", **fake_context)

        assert result is True
        # The recipient comes from the synapse_team_id param, not a hard-coded ID
        mock_synapse_client.sendMessage.assert_called_once_with(
            ["1234567"], "Tower workflow completed", "Test message"
        )


def test_every_task_alerts_on_failure() -> None:
    """Tests that a failure in any task, including the notification tasks, sends an alert."""
    for task in dag.tasks:
        assert task.on_failure_callback, f"{task.task_id} has no on_failure_callback"


def test_task_failure_alerts_dev_user_list(
    fake_context: dict[str, dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests that a failing task emails the dev_user_list with the task and exception."""
    sent: dict[str, Any] = {}

    def mock_send(conn_id: str, usernames: list[str], subject: str, body: str) -> None:
        sent.update(conn_id=conn_id, usernames=usernames, subject=subject, body=body)

    monkeypatch.setattr(synapse_alerts, "send_synapse_message", mock_send)

    class FailedTaskInstance:
        task_id = "generate_message"
        dag_id = dag.dag_id
        run_id = "manual__2026-10-19"
        log_url = "https://airflow.example.org/log"

    callback = dag.get_task("generate_message").on_failure_callback
    callback = callback[0] if isinstance(callback, list) else callback
    callback(
        {
            "params": {**fake_context["params"], "dev_user_list": "alice,bob"},
            "task_instance": FailedTaskInstance(),
            "exception": AttributeError("'NoneType' object has no attribute 'get'"),
        }
    )

    assert sent["conn_id"] == fake_context["params"]["synapse_conn_id"]
    assert sent["usernames"] == ["alice", "bob"]
    assert "generate_message" in sent["body"]
    assert "'NoneType' object has no attribute 'get'" in sent["body"]

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dags.agora_nf import dag
from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import WorkflowState, WorkflowStatus


RUN_ID = "tw-run"
RUN_NAME = "test_run"
DATASET = "test_dataset1, test_dataset2"

mock_ops_client = MagicMock()
mock_ops_client.launch_workflow.return_value = RUN_ID


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
        }
    }


@patch.object(NextflowTowerHook, 'ops', new=mock_ops_client)
def test_launch_agora_on_tower(fake_context: dict[str, dict[str, Any]]) -> None:
    """Tests the launch_agora_on_tower task with input parameters."""

    # Task functions are wrapped by the @task decorator; python_callable is the underlying function
    raw_python_function = dag.get_task("launch_agora_on_tower").python_callable
    run_id = raw_python_function(**fake_context)
    mock_ops_client.launch_workflow.assert_called_once()
    assert run_id == RUN_ID

    # Validate the full LaunchInfo payload and the remaining launch_workflow call args
    called_args, called_kwargs = mock_ops_client.launch_workflow.call_args
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
@patch.object(NextflowTowerHook, 'ops', new=mock_ops_client)
def test_monitor_agora_workflow_state(fake_context: dict[str, dict[str, Any]], state: WorkflowState) -> None:
    """Tests that monitor_nf_agora_workflow reports done only for terminal workflow states."""
    mock_ops_client.reset_mock()
    mock_ops_client.get_workflow.return_value.status = WorkflowStatus(state=state)

    raw_python_function = dag.get_task("monitor_nf_agora_workflow").python_callable
    workflow_state = raw_python_function(run_id=RUN_ID, **fake_context)

    # RUNNING is the only non-terminal state in this list, so it's the only one expecting False
    if state == WorkflowState.RUNNING:
        assert workflow_state is False
    else:
        assert workflow_state is True
    mock_ops_client.get_workflow.assert_called_once_with(RUN_ID)


@pytest.mark.parametrize(
    "state",
    [WorkflowState.SUCCEEDED, WorkflowState.FAILED, WorkflowState.CANCELLED, WorkflowState.UNKNOWN],
)
@pytest.mark.parametrize(
    "dataset_param, expected_dataset",
    [(DATASET, DATASET), (None, "all datasets"), ("", "all datasets")],
)
@patch.object(NextflowTowerHook, 'ops', new=mock_ops_client)
def test_generate_message(
    fake_context: dict[str, dict[str, Any]],
    state: WorkflowState,
    dataset_param: str | None,
    expected_dataset: str,
) -> None:
    """Tests that generate_message returns a string containing the run_id."""
    mock_ops_client.reset_mock()
    mock_ops_client.get_workflow.return_value.status = WorkflowStatus(state=state)
    mock_ops_client.get_workflow.return_value.run_name = RUN_NAME
    mock_ops_client.get_workflow.return_value.id = RUN_ID
    mock_ops_client.get_workflow.return_value.params = {"dataset": dataset_param}

    # Both submit and complete must be set for generate_message to compute a duration
    # instead of falling back to "unknown"
    submit = datetime(2026, 1, 1, 0, 0, 0)
    complete = datetime(2026, 1, 1, 1, 30, 0)
    mock_ops_client.get_workflow.return_value.submit = submit
    mock_ops_client.get_workflow.return_value.complete = complete

    raw_python_function = dag.get_task("generate_message").python_callable
    message = raw_python_function(run_id=RUN_ID, **fake_context)

    assert f"Tower workflow (Name: {RUN_NAME}, Id: {RUN_ID}) has completed with state: {state.value}" in message
    assert f"Duration (submission to completion): {complete - submit}" in message
    assert f"Dataset: {expected_dataset}" in message
"""Standalone integration smoke test for NextflowTowerHook.

Launches the trivial nextflow-io/hello pipeline on Nextflow Tower and polls it
to completion, to verify that NextflowTowerHook can authenticate, launch, and
monitor a workflow end-to-end against a real Tower workspace, without needing
a real bioinformatics pipeline or waiting on a long-running job.

This is a plain runnable script, not a pytest test: it hits a real external
service and requires real credentials for `tower_conn_id`.

Run directly with: python3 tests/integration/test_nextflow_tower_hook.py
"""
import sys
from datetime import datetime
from pathlib import Path

# dags/ must be on the path too, since dags/src/nextflow_tower_hook.py imports
# `from src.utils import ...` (src -> dags/src).
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "dags"))

from airflow.decorators import dag, task
from airflow.models import Param

from src.nextflow_tower_hook import LaunchInfo, NextflowTowerHook


dag_params = {
    "tower_conn_id": Param("EXAMPLE_DEV_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param("nf-hello-test", type="string"),
    "tower_compute_env_type": Param("manual-shared-ce-prod-project-spot-v13", type="string"),
}

dag_config = {
    "schedule": None,
    "start_date": datetime(2023, 6, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["nextflow_tower"],
    "params": dag_params,
}

@dag(**dag_config)
def nf_hello_test_dag():
    """Smoke-test NextflowTowerHook by launching and monitoring nextflow-io/hello.

    This DAG launches the trivial nextflow-io/hello pipeline on Nextflow Tower
    and polls it to completion, to verify that NextflowTowerHook can
    authenticate, launch, and monitor a workflow end-to-end without needing a
    real bioinformatics pipeline.

    DAG Parameters:

    - `tower_conn_id`: Connection ID for the Nextflow Tower workspace.
    - `tower_run_name`: Name assigned to the Tower workflow run.
    - `tower_compute_env_type`: Substring filter used to pick an AVAILABLE
        compute environment (see `NextflowTowerHook.get_latest_compute_env`).
    """

    @task()
    def launch_nf_hello_on_tower(**context):
        """Launch nextflow-io/hello on Nextflow Tower.

        Arguments:
            context: Airflow context dictionary containing DAG parameters
                - tower_conn_id: Connection ID for the Nextflow Tower workspace
                - tower_compute_env_type: Substring filter for the compute
                  environment to launch on

        Returns:
            str: Tower workflow run ID.
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name="nf-hello-test",
            pipeline="nextflow-io/hello",
            revision="master",
        )
        run_id = hook.launch_workflow(info, context["params"]["tower_compute_env_type"], ignore_previous_runs=True)
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_nf_hello_workflow(run_id: str, **context):
        """Poll the Tower workflow until it reaches a terminal state.

        Arguments:
            run_id: Tower run ID returned by launch_nf_hello_on_tower.
            context: Airflow context dictionary containing DAG parameters
                - tower_conn_id: Connection ID for the Nextflow Tower workspace

        Returns:
            bool: True once the workflow has reached a terminal state
                (success, failure, or cancellation), signaling the sensor to
                stop poking.
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    run_id = launch_nf_hello_on_tower()
    monitor_nf_hello_workflow(run_id=run_id)


dag = nf_hello_test_dag()

if __name__ == "__main__":
    dag.test()

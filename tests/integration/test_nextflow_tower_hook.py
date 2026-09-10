"""Run directly with: python3 tests/integration/test_nextflow_tower_hook.py"""
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
    @task()
    def launch_nf_hello_on_tower(**context):
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
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    run_id = launch_nf_hello_on_tower()
    monitor_nf_hello_workflow(run_id=run_id)


dag = nf_hello_test_dag()

if __name__ == "__main__":
    dag.test()

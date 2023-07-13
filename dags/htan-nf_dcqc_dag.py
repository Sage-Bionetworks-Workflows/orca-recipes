from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.decorators import dag, task
from airflow.models.param import Param

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo


dag_params = {
    "tower_conn_id": Param("HTAN_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param("airflow_test_dcqc", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
}

dag_config = {
    "schedule_interval": None,
    "start_date": datetime(2023, 6, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["nextflow_tower_metrics"],
    "params": dag_params,
}

@dag(**dag_config)
def htan_nf_dcqc_dag():
    @task()
    def launch_nf_dcqc_on_tower(**context):
        """
        Launches nf-dcqc tower workflow
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name=context["params"]["tower_run_name"],
            pipeline="Sage-Bionetworks-Workflows/nf-dcqc",
            revision="main",
            profiles=["test"],
            params={
                "outdir": "s3://htan-project-tower-bucket/dag_test/outputs",
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )
        run_id = hook.ops.launch_workflow(info, context["params"]["tower_compute_env_type"])
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_nf_dcqc_workflow(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    run_id = launch_nf_dcqc_on_tower()
    monitor_nf_dcqc_workflow(run_id=run_id)



htan_nf_dcqc_dag()

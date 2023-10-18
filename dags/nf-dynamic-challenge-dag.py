from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Param

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo


dag_params = {
    "tower_conn_id": Param("DYNAMIC_CHALLENGE_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param("dynamic-challenge-evaluation", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
    "challenge_view_id": Param("syn52658661", type="string"),
    "tower_cpus": Param("4", type="string"),
    "tower_memory": Param("16.GB", type="string"),
}

dag_config = {
    "schedule_interval": None,
    "start_date": datetime(2023, 6, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["nextflow_tower"],
    "params": dag_params,
}


@dag(**dag_config)
def nf_dynamic_challenge_dag():
    @task()
    def launch_nf_dynamic_challenge_on_tower(**context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name=context["params"]["tower_run_name"],
            pipeline="Sage-Bionetworks-Workflows/nf-synapse-challenge",
            revision="main",
            params={
                "view_id": context["params"]["challenge_view_id"],
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            entry_name="DATA_TO_MODEL_CHALLENGE",
        )
        run_id = hook.ops.launch_workflow(
            info, context["params"]["tower_compute_env_type"]
        )
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_nf_hello_workflow(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    run_id = launch_nf_dynamic_challenge_on_tower()
    monitor_nf_hello_workflow(run_id=run_id)


nf_dynamic_challenge_dag()

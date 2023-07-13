from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Param

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo


dag_params = {
    "tower_conn_id": Param("EXAMPLE_DEV_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param("nf-hello-test", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
}

dag_config = {
    "schedule_interval": None,
    "start_date": datetime(2023, 6, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["launched-by-orca", "airflow"],
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
            profiles=["sage"]
        )
        run_id = hook.ops.launch_workflow(info, context["params"]["tower_compute_env_type"])
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_model2data_workflow(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    run_id = launch_nf_hello_on_tower()
    monitor_model2data_workflow(run_id=run_id)


nf_hello_test_dag()

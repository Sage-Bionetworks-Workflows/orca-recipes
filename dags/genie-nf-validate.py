from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo

dag_params = {
    "tower_conn_id": Param("GENIE_BPC_PROJECT_TOWER_CONN", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
    "tower_run_name": Param("airflow-genie-validate", type="string"),
    "pipeline": Param("Sage-Bionetworks-Workflows/nf-genie", type="string"),
    "revision": Param("main", type="string"),
    "profile": Param("aws_prod", type="string"),
    "only_validate": Param("true", type="string"),
    "production": Param("true", type="string"),
    "release": Param("13.3-consortium", type="string"),
    "work_dir": Param("s3://genie-bpc-project-tower-scratch/1days", type="string")
}

dag_config = {
    "schedule_interval": "0 1,17 * * *",
    "start_date": datetime(2023, 2, 21),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["nextflow_tower"],
    "params": dag_params,
}


@dag(**dag_config)
def genie_nf_validate_dag():
    @task()
    def launch_nf_genie_on_tower(**context):
        """
        Launches tower workflow

        Args:
            workspace_id (str): Workspace ID for tower run
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name=context["params"]["tower_run_name"],
            pipeline=context["params"]["pipeline"],
            revision=context["params"]["revision"],
            work_dir=context["params"]["work_dir"],
            profiles=[context["params"]["profile"]],
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            params_yaml=f"""
                only_validate: {context["params"]["only_validate"]}
                production: {context["params"]["production"]}
                release: {context["params"]["release"]}
                """,
        )
        run_id = hook.ops.launch_workflow(info, context["params"]["tower_compute_env_type"])
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_nf_genie_workflow(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    run_id = launch_nf_genie_on_tower()
    monitor_nf_genie_workflow(run_id=run_id)

genie_nf_validate_dag = genie_nf_validate_dag()

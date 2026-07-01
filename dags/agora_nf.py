from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo

dag_params = {
    "tower_conn_id": Param("AGORA_PROJECT_TOWER_CONN", type="string"),
    "tower_compute_env_type": Param("agora-project-ondemand-v13", type="string"),
    "tower_run_name": Param("airflow-agora-model-ad", type="string"),
    "pipeline": Param("Sage-Bionetworks-Workflows/nf-agora", type="string"),
    "revision": Param("main", type="string"),
    "profile": Param("model_ad_preprod", type="string"),
    "work_dir": Param("s3://agora-project-tower-scratch/work", type="string"),
    "default_memory_gb": Param(32, type="integer"),
    "large_memory_gb": Param(64, type="integer"),
    "large_memory_datasets": Param("rna_de_individual,rna_de_aggregate", type="string"),
    "dataset": Param("", type="string"),
}

dag_config = {
    "schedule_interval": "0 5,17 * * *",
    "start_date": datetime(2023, 2, 21),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["nextflow_tower"],
    "params": dag_params,
}


@dag(**dag_config)
def agora_nf_run_dag():
    @task()
    def launch_agora_on_tower(**context):
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
            params={
                "default_memory_gb": context["params"]["default_memory_gb"],
                "large_memory_gb": context["params"]["large_memory_gb"],
                "large_memory_datasets": context["params"]["large_memory_datasets"],
                "dataset": context["params"]["dataset"],
            },
        )
        run_id = hook.ops.launch_workflow(
            info, context["params"]["tower_compute_env_type"], ignore_previous_runs=True
        )
        return run_id

    # @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    # def monitor_nf_genie_workflow(run_id: str, **context):
    #     hook = NextflowTowerHook(context["params"]["tower_conn_id"])
    #     workflow = hook.ops.get_workflow(run_id)
    #     print(f"Current workflow state: {workflow.status.state.value}")
    #     return workflow.status.is_done

    run_id = launch_agora_on_tower()
    # monitor_nf_genie_workflow(run_id=run_id)


agora_nf_run_dag = agora_nf_run_dag()
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models import Variable
from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from slack_sdk import WebClient

dag_params = {
    "tower_conn_id": Param("AGORA_PROJECT_TOWER_CONN", type="string"),
    "tower_compute_env_type": Param("agora-project-ondemand-v13", type="string"),
    "tower_run_name": Param("airflow-agora-model-ad", type="string"),
    "pipeline": Param("Sage-Bionetworks-Workflows/nf-agora", type="string"),
    "revision": Param("main", type="string"),
    "profile": Param("model_ad_preprod", type="string"),
    "work_dir": Param("s3://agora-project-tower-scratch/work", type="string"),
    "default_memory_gb": Param(32, type=["null", "integer"]),
    "large_memory_gb": Param(64, type=["null", "integer"]),
    "large_memory_datasets": Param("rna_de_individual,rna_de_aggregate", type=["null", "string"]),
    "dataset": Param(None, type=["null", "string"]),
}

dag_config = {
    "schedule_interval": None,
    # "start_date": datetime(2023, 2, 21),
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
        dataset = context["params"].get("dataset") or ""
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
                "dataset": dataset,
            },
        )
        run_id = hook.ops.launch_workflow(
            info, context["params"]["tower_compute_env_type"], ignore_previous_runs=True
        )
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_nf_genie_workflow(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    @task
    def generate_message(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)

        emoji = "🎉" if workflow.status.is_successful else "❌"
        dataset = workflow.params.get("dataset") or "all datasets"

        duration = "unknown"
        if workflow.submit and workflow.complete:
            duration = str(workflow.complete - workflow.submit)

        message = (
            f"{emoji} Tower workflow (Name: {workflow.run_name}, Id: {workflow.id}) "
            f"has completed with state: {workflow.status.state.value}\n"
            f"Dataset: {dataset}\n"
            f"Duration: {duration}\n"
        )

        config = hook.ops.config
        workspace = config.workspace or ""
        host = (config.api_endpoint or "").removesuffix("/api")
        if host and "/" in workspace:
            org_name, workspace_name = workspace.split("/", 1)
            message += f"\nView run: {host}/orgs/{org_name}/workspaces/{workspace_name}/watch/{workflow.id}"

        return message

    @task
    def post_slack_messages(message: str) -> bool:
        """Post the top downloads to the slack channel."""
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        result = client.chat_postMessage(channel="test-agora-nextflow", text=message)
        print(f"Result of posting to slack: [{result}]")
        return result is not None
    
    run_id = launch_agora_on_tower()
    monitor_task = monitor_nf_genie_workflow(run_id=run_id)
    message = generate_message(run_id=run_id)
    post_to_slack = post_slack_messages(message=message)
    
    run_id >> monitor_task >> message >> post_to_slack
    

agora_nf_run_dag()
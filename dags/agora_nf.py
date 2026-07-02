from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models import Variable
from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from slack_sdk import WebClient
from orca.services.synapse import SynapseHook

TOWER_HOST = "https://tower.sagebionetworks.org"
TOWER_ORG_NAME = "Sage-Bionetworks"
TOWER_WORKSPACE_NAME = "agora-project"
SLACK_CHANNEL = "test-agora-nextflow"
SYNAPSE_TEAM_ID = "3600433" # my test team id for now

dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "tower_conn_id": Param("AGORA_PROJECT_TOWER_CONN", type="string"), # personal access token belongs to Lingling Peng
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
            f"Duration (submission to completion): {duration}\n"
        )

        message += (
            f"\nView run: {TOWER_HOST}/orgs/{TOWER_ORG_NAME}"
            f"/workspaces/{TOWER_WORKSPACE_NAME}/watch/{workflow.id}/v2/tasks"
        )

        return message

    @task
    def post_slack_messages(message: str) -> bool:
        """Post the top downloads to the slack channel."""
        client = WebClient(token=Variable.get("SLACK_DPE_TEAM_BOT_TOKEN"))
        result = client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
        print(f"Result of posting to slack: [{result}]")
        return result is not None
    
    @task
    def post_email_messages(message: str, **context) -> bool:
        """Post the top downloads to the email channel."""
        syn_client = SynapseHook(context["params"]["synapse_conn_id"]).client
        syn_client.sendMessage([SYNAPSE_TEAM_ID], "Tower workflow completed", message)
        print(f"Result of posting to email: [{message}]")
        return True

    
    run_id = launch_agora_on_tower()
    monitor_task = monitor_nf_genie_workflow(run_id=run_id)
    message = generate_message(run_id=run_id)
    post_to_slack = post_slack_messages(message=message)
    post_to_email = post_email_messages(message=message)
    
    run_id >> monitor_task >> message >> [post_to_slack, post_to_email]

    

agora_nf_run_dag()
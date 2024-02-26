from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Param

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook


dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "tower_conn_id": Param("DYNAMIC_CHALLENGE_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param("dynamic-challenge-evaluation", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
    "view_id": Param("syn52576179", type="string"),
    "testing_data": Param("syn51390589", type="string"),
    "scoring_script": Param("dynamic_challenge_score.py", type="string"),
    "validation_script": Param("dynamic_challenge_validate.py", type="string"),
    "email_with_score": Param("yes", type="string"),
    "synapse_evaluation_id": Param("9615537", type="string"),
}

dag_config = {
    "schedule_interval": "* * * * *",
    "start_date": datetime(2023, 6, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["nextflow_tower"],
    "params": dag_params,
}


@dag(**dag_config)
def dynamic_challenge_dag():
    @task.branch()
    def check_for_new_submissions(**context):
        hook = SynapseHook(context["params"]["synapse_conn_id"])
        if hook.ops.monitor_evaluation_queue(
            context["params"]["synapse_evaluation_id"]
        ):
            return "launch_model2data_workflow"
        return "stop_dag"

    @task()
    def stop_dag():
        pass

    @task()
    def launch_data_to_model_on_tower(**context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name=context["params"]["tower_run_name"],
            pipeline="Sage-Bionetworks-Workflows/nf-synapse-challenge",
            revision="bwmac/ibcdpe-827/data_to_model_emails",
            profiles=["tower"],
            entry_name="DATA_TO_MODEL_CHALLENGE",
            params={
                "view_id": context["params"]["view_id"],
                # "scoring_script": context["params"]["scoring_script"],
                # "validation_script": context["params"]["validation_script"],
                "testing_data": context["params"]["testing_data"],
                "email_with_score": context["params"]["email_with_score"],
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
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

    submission_check = check_for_new_submissions()
    stop = stop_dag()
    run_id = launch_data_to_model_on_tower()
    monitor = monitor_nf_hello_workflow(run_id=run_id)

    submission_check >> [
        run_id,
        stop,
    ]
    run_id >> monitor


dynamic_challenge_dag()

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Param

from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook


dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "synapse_evaluation_id": Param("9615531", type="string"),
    "tower_conn_id": Param("PEGS_CHALLENGE_PROJECT_TOWER_CONN", type="string"),
    "tower_run_name": Param("pegs_model_submission_evaluation", type="string"),
    "tower_view_id": Param("syn53239158", type="string"),
    "tower_input_id": Param("syn53239289", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
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
def pegs_challenge_submission_dag():
    @task.branch()
    def check_for_new_submissions(**context):
        """
        Checks for new submissions

        Args:
            evaluation_id (str): Evaluation ID for challenge
        """
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
    def launch_model2data_workflow(**context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        info = LaunchInfo(
            run_name=context["params"]["tower_run_name"],
            pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-synapse-challenge",
            revision="main",
            entry_name="MODEL_TO_DATA_CHALLENGE",
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            params={
                "view_id": context["params"]["tower_view_id"],
                "input_id": context["params"]["tower_input_id"],
            },
        )
        run_id = hook.ops.launch_workflow(
            info, context["params"]["tower_compute_env_type"]
        )
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_model2data_workflow(run_id: str, **context):
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    submission_check = check_for_new_submissions()
    stop = stop_dag()
    run_id = launch_model2data_workflow()
    monitor = monitor_model2data_workflow(run_id=run_id)

    submission_check >> [
        run_id,
        stop,
    ]
    run_id >> monitor


pegs_challenge_submission_dag()
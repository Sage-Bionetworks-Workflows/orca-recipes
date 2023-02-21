from datetime import datetime

from airflow.decorators import dag, task
from dag_content.nextflow_tower_functions import create_and_open_tower_workspace


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 11, 11),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["nextflow_tower"],
)
def nf_validate_test_dag():
    @task()
    def launch_tower_workflow(workspace_id: str):
        """
        Launches tower workflow

        Args:
            workspace_id (str): Workspace ID for tower run
        """
        tower_utils = create_and_open_tower_workspace(
            tower_secret_key="TOWER_ACCESS_TOKEN", platform="sage-dev", workspace_id=workspace_id
        )
        tower_utils.launch_workflow(
            compute_env_id="635ROvIWp5w17QVdRy0jkk",
            pipeline="Sage-Bionetworks-Workflows/nf-validate",
            revision="main",
            profiles=["docker"],
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )

    launch_tower_workflow("4034472240746")


nf_validate_test_dag = nf_validate_test_dag()

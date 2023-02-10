from datetime import datetime

from airflow.decorators import dag, task

from dag_content.nextflow_tower_functions import create_and_open_tower_workspace


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 2, 9),
    catchup=False,
    default_args={
        "retries": 1,
    },
    tags=["nextflow_tower"],
)
def genie_nf_validate_dag():
    @task()
    def launch_tower_workflow(workspace_id: str):
        """
        Launches tower workflow

        Args:
            workspace_id (str): Workspace ID for tower run
        """
        tower_utils = create_and_open_tower_workspace(
            tower_access_token="TOWER_ACCESS_TOKEN_GENIE", platform="sage-dev", workspace_id=workspace_id
        )
        tower_utils.launch_workflow(
            compute_env_id="1IEIFEJSVdDzQhwQlZRrHW",
            pipeline="Sage-Bionetworks-Workflows/nf-genie",
            run_name="airflow-genie-validate-test",
            revision="main",
            profiles=["aws_test"],
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            params_yaml=f"""
                only_validate: true,
                production: false,
                release: TEST.consortium
                """,
        )

    launch_tower_workflow(workspace_id="5355285966491")


genie_nf_validate_dag = genie_nf_validate_dag()

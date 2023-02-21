from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from dag_content.nextflow_tower_functions import create_and_open_tower_workspace

dag_params = {
    "compute_env_id": Param("tJLngX5lvcm4EKhCVtAT", type="string"),
    "pipeline": Param("Sage-Bionetworks-Workflows/nf-genie", type="string"),
    "run_name": Param("airflow-genie-validate", type="string"),
    "revision": Param("main", type="string"),
    "profile": Param("aws_prod", type="string"),
    "only_validate": Param("true", type="string"),
    "production": Param("true", type="string"),
    "release": Param("13.3-consortium", type="string"),
}


dag_config = {
    "schedule_interval": "@daily",
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
    def launch_tower_workflow(workspace_id: str, **context):
        """
        Launches tower workflow

        Args:
            workspace_id (str): Workspace ID for tower run
        """
        tower_utils = create_and_open_tower_workspace(
            tower_access_token="TOWER_ACCESS_TOKEN_GENIE",
            platform="sage",
            workspace_id=workspace_id,
        )

        tower_utils.launch_workflow(
            compute_env_id=context["params"]["compute_env_id"],
            pipeline=context["params"]["pipeline"],
            run_name=context["params"]["run_name"],
            revision=context["params"]["revision"],
            profiles=[context["params"]["profile"]],
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            params_yaml=f"""
                only_validate: {context["params"]["only_validate"]},
                production: {context["params"]["production"]},
                release: {context["params"]["release"]}
                """,
        )

    launch_tower_workflow(workspace_id="5355285966491")


genie_nf_validate_dag = genie_nf_validate_dag()

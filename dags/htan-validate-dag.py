from datetime import datetime

import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from services.NextflowTowerService import create_and_open_tower_workspace
from services.AWSService import upload_file_s3
from pathlib import Path

@dag(
    schedule_interval=None,
    start_date=datetime(2022, 11, 11),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["nextflow_tower"],
)
def htan_nf_dcqc_dag():
    @task()
    def get_synapse_input_file() -> Path:
        """
        Gets synapse file (input csv) from synapse and saves it in buffer.
        Passes along dict object with the path to the downloaded file and the name of the file.

        Returns:
            Path: Path object containing the path to the downloaded file and the name of the file.
        """
        # simple param passing from run with config - sets my test file as default for now
        syn_id = get_current_context()["params"].get("syn_id", "syn50919899")
        syn_token = Variable.get("SYNAPSE_AUTH_TOKEN")
        syn = synapseclient.login(authToken=syn_token)
        file_path = Path(syn.get(syn_id).path)
        return file_path

    @task()
    def stage_input_in_s3(file_path: str) -> str:
        """
        Uploads file to Nextflow Tower S3 bucket, returns string path to file
        Args:
            file_path (str): String containing the path to the downloaded file to be uploaded to S3

        Returns:
            s3_uri (str): Path to S3 bucket location of file
        """
        s3_uri = upload_file_s3(file_path=file_path, bucket_name="orca-dev-project-tower-bucket")
        return s3_uri


    @task()
    def launch_tower_workflow(workspace_id: str, s3_uri: str):
        """
        Launches tower workflow

        Args:
            tower_utils (sagetasks.nextflowtower.utils.TowerUtils): TowerUtils class instance
            workspace_id (str): Workspace ID for tower run
            s3_path (str): Path to S3 location of input file
        """
        tower_utils = create_and_open_tower_workspace(platform="sage-dev", workspace_id=workspace_id)
        tower_utils.launch_workflow(
            compute_env_id="635ROvIWp5w17QVdRy0jkk",
            pipeline="Sage-Bionetworks-Workflows/nf-dcqc",
            profiles=["docker"],
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            revision="main",
            params_yaml=f'''
                input: {s3_uri}
                outdir: s3://orca-dev-project-tower-scratch/07_days/nf-dcqc-airflow-test/
                '''
        )

    file_path = get_synapse_input_file()
    s3_uri = stage_input_in_s3(file_path=file_path)
    launch_tower_workflow(
        workspace_id="4034472240746",
        s3_uri=s3_uri,
    )


htan_nf_dcqc_dag = htan_nf_dcqc_dag()

from datetime import datetime

import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from tempfile import TemporaryDirectory
from dags.services.nextflow_tower_service import create_and_open_tower_workspace
from dags.services.aws_service import upload_file_s3
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
    def stage_input_synapse_to_s3() -> Path:
        """
        Gets synapse file (input csv) from synapse and saves it in temp_dir. Then stages file in S3
        Passes along path to S3 location of file and cleans up temp_dir.

        Returns:
            s3_uri (str): Path to S3 bucket location of file
        """
        syn_id = get_current_context()["params"].get("syn_id", "syn50919899")
        syn_token = Variable.get("SYNAPSE_AUTH_TOKEN")
        syn = synapseclient.login(authToken=syn_token)
        temp_dir = TemporaryDirectory()
        syn_file = syn.get(syn_id, downloadLocation=temp_dir.name)
        file_path = Path(syn_file.path)
        s3_uri = upload_file_s3(file_path=file_path, bucket_name="orca-dev-project-tower-bucket")
        temp_dir.cleanup()
        return s3_uri


    @task()
    def launch_tower_workflow(workspace_id: str, s3_uri: str):
        """
        Launches tower workflow

        Args:
            workspace_id (str): Workspace ID for tower run
            s3_uri (str): Path to S3 location of input file
        """
        tower_utils = create_and_open_tower_workspace(platform="sage-dev", workspace_id=workspace_id)
        tower_utils.launch_workflow(
            compute_env_id="635ROvIWp5w17QVdRy0jkk",
            pipeline="Sage-Bionetworks-Workflows/nf-dcqc",
            run_name="airflow_nf_dcqc_run",
            profiles=["docker"],
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            revision="main",
            params_yaml=f'''
                input: {s3_uri}
                outdir: s3://orca-dev-project-tower-scratch/07_days/nf-dcqc-airflow-test/
                '''
        )

    s3_uri = stage_input_synapse_to_s3()
    launch_tower_workflow(
        workspace_id="4034472240746",
        s3_uri=s3_uri,
    )


htan_nf_dcqc_dag = htan_nf_dcqc_dag()

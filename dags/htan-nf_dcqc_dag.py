from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dag_content.nextflow_tower_functions import create_and_open_tower_workspace
from dag_content.utils import create_synapse_session


def upload_file_s3(file_path: Path, bucket_name: str) -> str:
    """Uploads file from file_path to s3 bucket_name
    Args:
        file_path (str): Path to file to be uploaded
        bucket_name (str): Location in S3 for file to be uploaded
    
    Returns:
        str: uri pointing to new uploaded file location in s3
    """
    s3_hook = S3Hook(aws_conn_id="TOWER_DB_CONNECTION", region_name="us-east-1")
    s3_hook.load_file(filename=file_path, key=file_path.name, bucket_name=bucket_name, replace=True)
    return f"s3://{bucket_name}/{file_path.name}"

@dag(
    schedule_interval=None,
    start_date=datetime(2022, 11, 11),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["nextflow_tower_metrics"],
)
def htan_nf_dcqc_dag():
    @task()
    def stage_input_synapse_to_s3() -> str:
        """
        Gets synapse file (input csv) from synapse and saves it in temp_dir. Then stages file in S3
        Passes along path to S3 location of file and cleans up temp_dir.

        Returns:
            s3_uri (str): Path to S3 bucket location of file
        """
        syn_id = get_current_context()["params"].get("syn_id", "syn50919899")
        syn = create_synapse_session()
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
        tower_utils = create_and_open_tower_workspace(
            tower_secret_key="TOWER_ACCESS_TOKEN", platform="sage-dev", workspace_id=workspace_id
        )
        tower_utils.launch_workflow(
            compute_env_id="635ROvIWp5w17QVdRy0jkk",
            pipeline="Sage-Bionetworks-Workflows/nf-dcqc",
            run_name="airflow_nf_dcqc_run",
            profiles=["docker"],
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
            revision="main",
            params_yaml=f"""
                input: {s3_uri}
                outdir: s3://orca-dev-project-tower-scratch/07_days/nf-dcqc-airflow-test/
                """,
        )

    s3_uri = stage_input_synapse_to_s3()
    launch_tower_workflow(
        workspace_id="4034472240746",
        s3_uri=s3_uri,
    )


htan_nf_dcqc_dag = htan_nf_dcqc_dag()

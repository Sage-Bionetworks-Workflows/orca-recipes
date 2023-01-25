from datetime import datetime

import boto3
import synapseclient
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from dag_content.tower_metrics_content import AWS_CREDS, AWS_REGION
from sagetasks.nextflowtower.utils import TowerUtils


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
    @task(multiple_outputs=True)
    def get_synapse_file_path() -> dict:
        """
        Gets synapse file (input csv) from synapse and saves it in buffer. 
        Passes along dict object with the path to the downloaded file and the name of the file.

        Returns:
            dict: Dictionary containing the path to the downloaded file and the name of the file.
        """
        #simple param passing from run with config - sets my test file as default for now
        syn_id = get_current_context()["params"].get("syn_id") if get_current_context()["params"].get("syn_id") is not None else "syn50919899"
        syn_token = Variable.get("SYNAPSE_AUTH_TOKEN")
        syn = synapseclient.login(authToken=syn_token)
        file_path = syn.get(syn_id).path
        file_name = file_path.split("/")[-1]
        return {"file_path":file_path, "file_name":file_name}

    @task()
    def upload_file_to_s3(syn_file_dict:dict, aws_creds:dict, aws_region:str) -> str:
        """
        Uploads file to Nextflow Tower S3 bucket, returns string path to file
        Args:
            syn_file_dict (dict): Dictionary containing file_path and file_name from get_synapse_file_path
            aws_creds (dict): Dictionary containing AWS credentials
            aws_region (str): String containing AWS region

        Returns:
            url (str): Path to S3 bucket location of file
        """
        bucket_name = "example-dev-project-tower-bucket"
        file_path = syn_file_dict["file_path"]
        file_name = syn_file_dict["file_name"]

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_creds["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=aws_creds["AWS_SECRET_ACCESS_KEY"],
            region_name = aws_region
        )
        s3.upload_file(file_path, bucket_name, file_name)
        url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
        return url

    @task(multiple_outputs=True)
    def open_tower_workspace():
        """
        Opens tower workspace - things are hard coded for the moment that would be parameterized in future versions

        Returns:
            dict: TowerUtils class instance within dictionary for easy variable passing
        """
        tower_token = Variable.get("TOWER_ACCESS_TOKEN", default_var="undefined")
        client_args = TowerUtils.bundle_client_args(
            tower_token, platform="sage-dev", debug_mode=False
        )
        tower_utils = TowerUtils(client_args)
        return {"tower_utils": tower_utils}

    @task()
    def launch_tower_workflow(tower_utils: TowerUtils, workspace_id: str, s3_path:str):
        """
        Launches tower workflow

        Args:
            tower_utils (sagetasks.nextflowtower.utils.TowerUtils): TowerUtils class instance
            workspace_id (str): Workspace ID for tower run
            s3_path (str): Path to S3 location of input file
        """
        tower_utils.open_workspace(workspace_id)
        tower_utils.launch_workflow(
            compute_env_id="635ROvIWp5w17QVdRy0jkk",
            pipeline="Sage-Bionetworks-Workflows/nf-dcqc",
            run_name="nf-dcqc-test",
            params_json={"input_path_s3": s3_path, "required_tests": [], "skip_tests": []}
        )

    tower_utils = open_tower_workspace()
    file_info = get_synapse_file_path()
    s3_path = upload_file_to_s3(syn_file_dict=file_info, aws_creds=AWS_CREDS, aws_region=AWS_REGION)
    launch_tower_workflow(tower_utils=tower_utils["tower_utils"], workspace_id="4034472240746", s3_path=s3_path)


htan_nf_dcqc_dag = htan_nf_dcqc_dag()

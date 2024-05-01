import os
import uuid

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from orca.services.synapse import SynapseHook
from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo

from synapseclient.models import File

REGION_NAME = "us-east-1"
BUCKET_NAME = "example-dev-project-tower-scratch"
KEY = "10days/dcqc"


dag_params = {
    "tower_conn_id": Param("EXAMPLE_DEV_PROJECT_TOWER_CONN", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "synapse_container": Param("syn58807287", type="string"),
    "manifest_suffix": Param("dcqc_manifest.csv", type="string"),
    "aws_conn_id": Param("AWS_TOWER_DEV_S3_CONN", type="string"),
    "uuid": Param(str(uuid.uuid4()), type="string"),
}

dag_config = {
    "schedule_interval": "0 0 * * *",
    "start_date": datetime(2023, 6, 1),
    "catchup": False,
    "default_args": {
        "retries": 2,
    },
    "tags": ["nextflow_tower_metrics"],
    "params": dag_params,
}

@dag(**dag_config)
def dcqc_poc_dag():
    @task()
    def get_new_manifest_entities(**context):
        """
        Gets a list of the any manifest files in the synapse_container that have been uploaded in the last 24 hours
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        container = syn_hook.client.get(context["params"]["synapse_container"])

        if container.concreteType not in ["org.sagebionetworks.repo.model.Folder", "org.sagebionetworks.repo.model.Project"]:
            raise ValueError(f"Provided Synapse entity must be a Folder or Project, entity provided is '{container.concreteType}'")

        child_entities = syn_hook.client.getChildren(container, includeTypes=["file"])
        manifest_entities = [child for child in child_entities if child["name"].endswith(context["params"]["manifest_suffix"]) and datetime.strptime(child["modifiedOn"], "%Y-%m-%dT%H:%M:%S.%fZ") > datetime.now() - timedelta(days=1)]
        return manifest_entities
    
    @task.branch()
    def check_number_of_manifest_entities(manifest_entities: list, **context):
        """
        Check that there is only one manifest file in the synapse_container.
        If 0, continue to the stop_dag task.
        If 1, continue to the stage_manifest task.
        If >1, raise an error.
        """
        if len(manifest_entities) == 1:
            return "stage_manifest"
        if len(manifest_entities) == 0:
            return "stop_dag"
        if len(manifest_entities) > 1:
            raise ValueError(f"More than one manifest entity found in {context['params']['synapse_container']}.")
        
    @task()
    def stop_dag() -> None:
        pass

    @task()
    def stage_manifest(manifest_entities: list, **context):
        """
        Stage the manifest file in S3.
        """
        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        manifest_file = syn_hook.client.get(manifest_entities[0]["id"])

        s3_hook = S3Hook(
            aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME
        )
        run_uuid = context["params"]["uuid"]
        s3_hook.load_file(
            filename=manifest_file.path, key=f"{KEY}/{run_uuid}/{manifest_file.name}", bucket_name=BUCKET_NAME
        )
        return f"s3://{BUCKET_NAME}/{KEY}/{run_uuid}/{manifest_file.name}"

    @task()
    def launch_dcqc(manifest_uri: str, **context):
        """
        Launches nf-dcqc tower workflow
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        parent = context["params"]["synapse_container"]
        run_uuid = context["params"]["uuid"]
        info = LaunchInfo(
            run_name=f"DCQC_{parent}_{run_uuid}",
            pipeline="Sage-Bionetworks-Workflows/nf-dcqc",
            revision="main",
            params={
                "input": manifest_uri,
                "outdir": f"s3://{BUCKET_NAME}/{KEY}/{run_uuid}",
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )
        run_id = hook.ops.launch_workflow(info, context["params"]["tower_compute_env_type"])
        return run_id

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_dcqc(run_id: str, **context):
        """
        Monitor the nf-dcqc tower workflow.
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done
    
    @task()
    def upload_output_file_to_synapse(**context):
        """
        Download the DCQC output file and upload it to Synapse.
        """
        s3_hook = S3Hook(
            aws_conn_id=context["params"]["aws_conn_id"], region_name=REGION_NAME
        )
        run_uuid = context["params"]["uuid"]
        output_file = s3_hook.download_file(
            key=f"{KEY}/{run_uuid}/output.csv", bucket_name=BUCKET_NAME
        )

        os.rename(output_file, "dcqc_output.csv")

        syn_hook = SynapseHook(context["params"]["synapse_conn_id"])
        File(
            path="dcqc_output.csv",
            parent_id=context["params"]["synapse_container"],
            description="DCQC run results"
        ).store(synapse_client=syn_hook.client)

        os.remove("dcqc_output.csv")

    manifest_entities = get_new_manifest_entities()
    manifest_entities_checked = check_number_of_manifest_entities(manifest_entities=manifest_entities)
    stop = stop_dag()
    manifest_uri = stage_manifest(manifest_entities=manifest_entities)
    run_id = launch_dcqc(manifest_uri=manifest_uri)
    complete_run = monitor_dcqc(run_id=run_id)
    output_uploaded = upload_output_file_to_synapse()

    manifest_entities >> manifest_entities_checked >> [stop, manifest_uri]
    manifest_uri >> run_id >> complete_run >> output_uploaded


dcqc_poc_dag()

"""ORCA recipe: nf-core/sarek WES somatic variant calling (tumor vs. matched normal), GRCh38.
Runs on Nextflow Tower and returns results to Synapse.

Orchestrates:
  0. wait_for_record_set  : Poll the ComputeTask's RecordSet until task_status=COMPLETE
  1. fetch_samplesheet    : Materialize the samplesheet from the RecordSet to S3
  2. nf-synapse synstage  : Download FASTQ files from Synapse to S3
  3. sarek                : Run nf-core/sarek somatic variant calling
  4. nf-synapse synindex  : Index results back to Synapse
  5. record_provenance    : Attach Synapse Activity provenance to the outputs

Somatic mode is set by the samplesheet, not a flag: a tumor (status=1) and matched
normal (status=0) sharing a patient id trigger tumor-vs-normal calling.

Config reproduces the JHU NF1 Biobank release-2 run (JH_batch1): sarek 3.1.2,
GATK.GRCh38, WES + Agilent V6 intervals, callers strelka,mutect2,vep.
Ref: https://github.com/nf-osi/biobank-release-2

The recordSetId, launch info, and task_status all come from a temporary
`ComputeTask` shim (`src/compute_task.py`) that stands in for the not-yet-built
Synapse Data Processing Compute Task; migrating later = swapping that shim's
method bodies, not this DAG.

Prerequisites:
  - SYNAPSE_AUTH_TOKEN set as a Tower workspace secret (not a user secret).
  - Airflow connections configured for Synapse, Nextflow Tower, and AWS.
"""

import csv
import re
import tempfile
from datetime import datetime
from typing import Any

import synapseutils
from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from orca.services.nextflowtower import NextflowTowerHook
from orca.services.synapse import SynapseHook
from synapseclient import Activity

from src.compute_task import RECORD_SET_READY_STATUS, ComputeTask


def extract_fastq_synapse_ids(samplesheet_path: str) -> list[str]:
    """ Extract Synapse IDs from the fastq_1 and fastq_2 columns of a Sarek samplesheet CSV.

    Args:
        samplesheet_path (str): Path to the sarek samplesheet CSV file

    Returns:
        list[str]: List of Synapse IDs extracted from the fastq_1 and fastq_2 columns.
    """
    synapse_ids: list[str] = []
    with open(samplesheet_path, newline="") as handle:
        for row in csv.DictReader(handle):
            for column in ("fastq_1", "fastq_2"):
                value = (row.get(column) or "").strip()
                synapse_ids.extend(re.findall(r"syn\d+", value))
    # Preserve order while dropping duplicates.
    return list(dict.fromkeys(synapse_ids))


dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "tower_conn_id": Param("NTAP_ADD5_PROJECT_TOWER_CONN", type="string"),
    "tower_compute_env_type": Param("spot", type="string"),
    # AWS identity for S3 staging. Must have access to the ComputeTask's Tower
    # bucket, which lives in a different account than Airflow's secrets backend.
    "aws_conn_id": Param("AWS_TOWER_PROD_S3_CONN", type="string"),
    # Which compute task to run. Everything else: record_set_id, output_folder_id,
    # bucket_name, staging_key, institution, run_number, launch info, task_status
    # (+ transitional samplesheet_* fields) is provided by ComputeTask.load()
    "compute_task_id": Param("bdf-sarek-jhu-demo", type="string"),
}

dag_config = {
    "schedule": None,
    "start_date": datetime(2026, 7, 15),
    "catchup": False,
    "default_args": {
        "retries": 0,
    },
    "tags": ["nextflow_tower", "synapse", "sarek"],
    "params": dag_params,
}


@dag(**dag_config)
def bdf_sarek_somatic_pipeline_dag() -> DAG:

    @task.sensor(poke_interval=10, timeout=60 * 60 * 24, mode="poke")
    def wait_for_record_set(**context: Any) -> bool:
        """Poll the ComputeTask's RecordSet every 10s until it is ready.

        The ComputeTask only provides the status; the readiness decision (is it
        COMPLETE?) is this DAG's.

        Returns:
            bool: Whether the RecordSet with samplesheet is ready for processing
        """
        params = context["params"]
        compute_task = ComputeTask.load(params["compute_task_id"])
        syn = SynapseHook(params["synapse_conn_id"]).client
        status = compute_task.task_status(syn)
        print(f"compute task record_set {compute_task.record_set_id} task_status={status}")
        return status == RECORD_SET_READY_STATUS

    @task()
    def fetch_samplesheet(**context: Any) -> None:
        """Materialize the samplesheet from the ComputeTask's RecordSet and upload it
        to the S3 `to_stage` location where synstage reads it.

        (The prior method fetched a curated samplesheet CSV file by its synID; the
        RecordSet is now the source. `ComputeTask` owns both the recordSetId and
        the S3 paths.)
        """
        from synapseclient.models import RecordSet

        params = context["params"]
        compute_task = ComputeTask.load(params["compute_task_id"])
        syn = SynapseHook(params["synapse_conn_id"]).client
        with tempfile.TemporaryDirectory() as tmp_dir:
            record_set = RecordSet(id=compute_task.record_set_id, path=tmp_dir).get(
                synapse_client=syn
            )
            S3Hook(aws_conn_id=params["aws_conn_id"]).load_file(
                filename=record_set.path,
                key=compute_task.samplesheet_to_stage_key,
                bucket_name=compute_task.bucket_name,
                replace=True,
            )

    @task()
    def launch_synstage(**context: Any) -> str:
        """Launch nf-synapse synstage to download the samplesheet's FASTQs to S3.

        Returns:
            str: The Nextflow Tower run ID of the launched synstage workflow
        """
        compute_task = ComputeTask.load(context["params"]["compute_task_id"])
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        return hook.ops.launch_workflow(
            compute_task.synstage_launch_info(),
            context["params"]["tower_compute_env_type"],
            ignore_previous_runs=True,
        )

    @task()
    def launch_sarek(**context: Any) -> str:
        """Launch nf-core/sarek somatic variant calling workflow.

        Returns:
            str: The Nextflow Tower run ID of the launched nf-core/sarek workflow
        """
        compute_task = ComputeTask.load(context["params"]["compute_task_id"])
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        return hook.ops.launch_workflow(
            compute_task.sarek_launch_info(),
            context["params"]["tower_compute_env_type"],
            ignore_previous_runs=True,
        )

    @task()
    def launch_synindex(**context: Any) -> str:
        """Launch nf-synapse synindex to index sarek results back into Synapse.

        Returns:
            str: The Nextflow Tower run ID of the launched synindex workflow
        """
        compute_task = ComputeTask.load(context["params"]["compute_task_id"])
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        return hook.ops.launch_workflow(
            compute_task.synindex_launch_info(),
            context["params"]["tower_compute_env_type"],
            ignore_previous_runs=True,
        )

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_workflow(tower_run_id: str, **context: Any) -> bool:
        """Poll Tower until the given run reaches a terminal state.

        Returns:
            bool: True if the workflow has reached a terminal state, False otherwise
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(tower_run_id)
        print(f"Current workflow state: {workflow.status.state.value}")
        return workflow.status.is_done

    @task()
    def record_provenance(
        synstage_run_id: str,
        sarek_run_id: str,
        synindex_run_id: str,
        **context: Any,
    ) -> None:
        """Attach minimal Synapse Activity provenance to the indexed sarek outputs.

        used = the input samplesheet synID and the original input FASTQ synIDs;
        the pipeline config and Tower run IDs are recorded in the activity
        description. The activity is set on every file synindex indexed under the
        output folder.

        Args:
            synstage_run_id (str): The Nextflow Tower run ID of the launched synstage workflow.
            sarek_run_id (str): The Nextflow Tower run ID of the launched nf-core/sarek workflow.
            synindex_run_id (str): The Nextflow Tower run ID of the launched synindex workflow.
        """
        params = context["params"]
        compute_task = ComputeTask.load(params["compute_task_id"])
        syn = SynapseHook(params["synapse_conn_id"]).client

        samplesheet_file = syn.get(
            compute_task.samplesheet_id, version=compute_task.samplesheet_version
        )
        fastq_ids = extract_fastq_synapse_ids(samplesheet_file.path)
        used = [compute_task.samplesheet_id, *fastq_ids]

        activity = Activity(
            name=compute_task.sarek_run_name,
            description=(
                "nf-core/sarek 3.1.2 somatic variant calling "
                "(genome=GATK.GRCh38, wes=True, tools=strelka,mutect2,vep, "
                f"intervals={compute_task.intervals}, institution={compute_task.institution}). "
                f"Input samplesheet: {compute_task.samplesheet_id}. "
                f"Input FASTQs: {fastq_ids}. "
                f"Tower runs: synstage={synstage_run_id}, sarek={sarek_run_id}, "
                f"synindex={synindex_run_id}."
            ),
            used=used,
            executed=["https://github.com/nf-core/sarek"],
        )

        stored = None
        indexed = 0
        for _dirpath, _dirnames, filenames in synapseutils.walk(
            syn, compute_task.output_folder_id
        ):
            for _name, syn_id in filenames:
                stored = syn.setProvenance(syn_id, stored or activity)
                indexed += 1
        print(
            f"Set provenance on {indexed} output file(s) under "
            f"{compute_task.output_folder_id}; used={used}"
        )

    ready = wait_for_record_set()
    fetch = fetch_samplesheet()

    synstage_run_id = launch_synstage()
    synstage_done = monitor_workflow.override(task_id="monitor_synstage")(synstage_run_id)

    sarek_run_id = launch_sarek()
    sarek_done = monitor_workflow.override(task_id="monitor_sarek")(sarek_run_id)

    synindex_run_id = launch_synindex()
    synindex_done = monitor_workflow.override(task_id="monitor_synindex")(synindex_run_id)

    provenance = record_provenance(synstage_run_id, sarek_run_id, synindex_run_id)

    # Strict ordering: wait -> fetch -> synstage -> sarek -> synindex -> provenance,
    # each stage waiting for the previous to finish.
    ready >> fetch
    fetch >> synstage_run_id
    synstage_done >> sarek_run_id
    sarek_done >> synindex_run_id
    synindex_done >> provenance


dag = bdf_sarek_somatic_pipeline_dag()


if __name__ == "__main__":
    from src.utils import validate_required_secrets

    validate_required_secrets(
        connection_ids=[
            dag_params["tower_conn_id"].value,
            dag_params["synapse_conn_id"].value,
            dag_params["aws_conn_id"].value,
        ],
        variable_names=[],
    )
    # ComputeTask.load() provides record_set_id, run_number, etc.; for a local
    # rerun bump run_number in ComputeTask.load() (until the real task supplies it).
    dag.test(run_conf={"compute_task_id": "bdf-sarek-jhu-demo"})

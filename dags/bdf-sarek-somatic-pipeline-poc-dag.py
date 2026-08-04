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
ComputeTask (`src/bdf_compute_task.py`) that stands in for the not-yet-built
Synapse Data Processing Compute Task; migrating later = swapping that shim layer's
method bodies, not this DAG.

Prerequisites:
  - SYNAPSE_AUTH_TOKEN set as a Tower workspace secret (not a user secret).
  - Airflow connections configured for Synapse, Nextflow Tower, and AWS.
"""

import csv
import json
import os
import re
import tempfile
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from orca.services.nextflowtower import NextflowTowerHook
from synapseclient import Activity, File

from src.synapse_hook import SynapseHook
from src.bdf_local_compute_task import CURATION_TASK_READY_STATUS, ComputeTask
from src.utils import get_logger

logger = get_logger(__name__)


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


def input_fastq_ids_from_record_set(syn: "synapseclient.Synapse", record_set_id: str) -> list[str]:
    """Extract the input fastq synIDs from the ComputeTask's input RecordSet.

    This downloads the RecordSet's samplesheet and parses its input fastq syn:// URIs,
    the actual inputs the run consumed, so no separate samplesheet file or
    output-folder walk is needed.

    Args:
        syn (synapseclient.Synapse): Logged-in Synapse client.
        record_set_id (str): The input RecordSet's synID (from ComputeTask).

    Returns:
        list[str]: Deduped input synIDs, in samplesheet order.
    """
    from synapseclient.models import RecordSet

    with tempfile.TemporaryDirectory() as tmp_dir:
        record_set = RecordSet(id=record_set_id, path=tmp_dir).get(synapse_client=syn)
        return extract_fastq_synapse_ids(record_set.path)


def fetch_tower_run_config(ops: "NextflowTowerOps", run_id: str) -> dict[str, Any]:
    """Fetch the actual launch config Tower recorded for a workflow run.

    Uses Tower's describe-launch endpoint (GET /workflow/{id}/launch), which
    returns the config as launched: configText, paramsText, profiles, revision,
    workDir, pipeline i.e. the authoritative per-run config Tower used, not
    just what we submitted.

    Args:
        ops (NextflowTowerOps): (hook.ops), for its client + workspace id.
        run_id (str): Tower workflow run ID

    Returns:
        dict[str, Any]: The run's launch config.
    """
    launch = ops.client.get(
        f"/workflow/{run_id}/launch",
        params={"workspaceId": ops.workspace_id},
    )["launch"]
    return {
        "pipeline": launch.get("pipeline"),
        "revision": launch.get("revision"),
        "profiles": launch.get("configProfiles"),
        "work_dir": launch.get("workDir"),
        "params_text": launch.get("paramsText"),
        "config_text": launch.get("configText"),
    }


def submitted_run_configs(compute_task: ComputeTask) -> dict[str, Any]:
    """Fallback: the configs we SUBMITTED for each stage (from ComputeTask launch info).

    Used when `fetch_tower_run_config` can't retrieve the launched config from
    Tower (e.g. the API shape differs). This is what we sent, not necessarily
    what Tower resolved, but it keeps provenance populated.

    Args:
        compute_task (ComputeTask): Provides the per-stage LaunchInfo.

    Returns:
        dict[str, Any]: Per-stage submitted configs keyed by stage name.
    """

    def as_dict(info: Any) -> dict[str, Any]:
        return {
            "pipeline": info.pipeline,
            "revision": info.revision,
            "profiles": info.profiles,
            "params": info.params,
            "config_text": info.nextflow_config,
            "pre_run_script": info.pre_run_script,
        }

    return {
        "synstage": as_dict(compute_task.synstage_launch_info()),
        "sarek": as_dict(compute_task.sarek_launch_info()),
        "synindex": as_dict(compute_task.synindex_launch_info()),
    }


def upload_run_configs(syn: "synapseclient.Synapse", compute_task: ComputeTask, run_configs: dict[str, Any]) -> str:
    """Upload the per-run Tower configs as one JSON file in the output folder.

    Args:
        syn (synapseclient.Synapse): Logged-in Synapse client.
        compute_task (ComputeTask): Provides the output folder + run name.
        run_configs (dict[str, Any]): Per-stage Tower configs keyed by stage name.

    Returns:
        str: synID of the uploaded config file.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(
            tmp_dir, f"{compute_task.sarek_run_name}_tower_run_configs.json"
        )
        with open(config_path, "w") as handle:
            json.dump(run_configs, handle, indent=2, sort_keys=True)
        stored = syn.store(File(config_path, parent=compute_task.output_folder_id))
    return stored.id


def record_run_provenance(
    syn: "synapseclient.Synapse",
    tower_ops: "NextflowTowerOps",
    compute_task: ComputeTask,
    synstage_run_id: str,
    sarek_run_id: str,
    synindex_run_id: str,
) -> str:
    """Build and attach the run's Synapse Activity provenance to the output folder.

    Free of Airflow: takes an explicit Synapse client and Tower ops so it can be
    unit-tested directly. Reads the input FASTQs from the RecordSet, captures the
    per-run configs (actual from Tower, else the submitted configs on failure),
    uploads them, and sets the Activity (used = RecordSet + FASTQs + config file;
    executed = nf-core/sarek) on the output folder.

    Args:
        syn (synapseclient.Synapse): Logged-in Synapse client.
        tower_ops (NextflowTowerOps): (hook.ops), for fetching run configs.
        compute_task (ComputeTask): Provides record_set_id, output folder, config.
        synstage_run_id (str): Tower run ID of the synstage workflow.
        sarek_run_id (str): Tower run ID of the nf-core/sarek workflow.
        synindex_run_id (str): Tower run ID of the synindex workflow.

    Returns:
        str: The output folder synID the Activity was set on.
    """
    fastq_ids = input_fastq_ids_from_record_set(syn, compute_task.record_set_id)

    # Capture the ACTUAL launched config Tower recorded for each of the three runs.
    # If the Tower endpoint can't be read, fall back to the configs we submitted.
    try:
        run_configs = {
            "synstage": fetch_tower_run_config(tower_ops, synstage_run_id),
            "sarek": fetch_tower_run_config(tower_ops, sarek_run_id),
            "synindex": fetch_tower_run_config(tower_ops, synindex_run_id),
        }
    except Exception as error:  # noqa: BLE001 - any Tower/API failure -> fallback
        logger.warning(f"Could not fetch run configs from Tower ({error!r}); using submitted configs.")
        run_configs = submitted_run_configs(compute_task)

    config_synapse_id = upload_run_configs(syn, compute_task, run_configs)
    used = [compute_task.record_set_id, *fastq_ids, config_synapse_id]

    activity = Activity(
        name=compute_task.sarek_run_name,
        description=(
            "nf-core/sarek 3.1.2 somatic variant calling "
            "(genome=GATK.GRCh38, wes=True, tools=strelka,mutect2,vep, "
            f"intervals={compute_task.intervals}, institution={compute_task.institution}). "
            f"Input RecordSet: {compute_task.record_set_id}. "
            f"Input FASTQs: {fastq_ids}. "
            f"Tower run configs (synstage/sarek/synindex): {config_synapse_id}. "
            f"Tower runs: synstage={synstage_run_id}, sarek={sarek_run_id}, "
            f"synindex={synindex_run_id}."
        ),
        used=used,
        executed=["https://github.com/nf-core/sarek"],
    )

    syn.setProvenance(compute_task.output_folder_id, activity)
    logger.info(f"Set provenance on output folder {compute_task.output_folder_id}; used={used}")
    return compute_task.output_folder_id


dag_params = {
    "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
    "tower_conn_id": Param("NTAP_ADD5_PROJECT_TOWER_CONN", type="string"),
    # run on spot, the long, interruption-sensitive sarek run uses ondemand.
    "tower_spot_compute_env_type": Param("spot", type="string"),
    "tower_ondemand_compute_env_type": Param("ondemand", type="string"),
    # AWS identity for S3 staging. Must have access to the ComputeTask's Tower
    # bucket, which lives in a different account than Airflow's secrets backend.
    "aws_conn_id": Param("AWS_TOWER_PROD_S3_CONN", type="string"),
    # Which compute task to run. Everything else: record_set_id, output_folder_id,
    # bucket_name, staging_key, institution, run_number, launch info, task_status
    # (+ transitional samplesheet_* fields) is provided by ComputeTask.load()
    "compute_task_id": Param("bdf-sarek-jhu-demo", type="string"),
}

dag_config = {
    # runs Mon-Fri at 14:00 UTC (7:00am PDT)
    "schedule": "0 14 * * 1-5",
    "max_active_runs": 1,
    "start_date": datetime(2026, 7, 15),
    "catchup": False,
    "default_args": {
        "retries": 0,
    },
    "tags": ["nextflow_tower", "synapse", "sarek"],
    "params": dag_params,
}


@dag(**dag_config)
def bdf_sarek_somatic_pipeline_poc_dag() -> DAG:
    """ Airflow DAG for the BDF Sarek somatic variant calling pipeline


    Raises:
        AirflowException: If any of the Tower runs end in a
            terminal non-success state (FAILED / CANCELLED / UNKNOWN)

    Returns:
        DAG: The Airflow DAG object for the BDF Sarek somatic pipeline
    """
    @task.sensor(poke_interval=10, timeout=60 * 60 * 6, mode="reschedule")
    def wait_for_record_set(**context: Any) -> bool:
        """Poll the ComputeTask's CurationTask every 10s until it is ready.
        Times out after 6 hours. The ComputeTask's CurationTask is ready when
        its state is COMPLETE.

        Returns:
            bool: Whether the CurationTask containing RecordSet with samplesheet
                is ready for processing
        """
        params = context["params"]
        compute_task = ComputeTask.load(params["compute_task_id"])
        syn = SynapseHook(params["synapse_conn_id"]).client
        status = compute_task.task_status(syn)
        logger.info(f"compute task record_set {compute_task.record_set_id} task_status={status}")
        return status == CURATION_TASK_READY_STATUS

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
            context["params"]["tower_spot_compute_env_type"],
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
            context["params"]["tower_ondemand_compute_env_type"],
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
            context["params"]["tower_spot_compute_env_type"],
            ignore_previous_runs=True,
        )

    @task.sensor(poke_interval=60, timeout=604800, mode="reschedule")
    def monitor_workflow(tower_run_id: str, **context: Any) -> bool:
        """Poll Tower until the run succeeds; fail if it ends non-successfully.

        Only a SUCCEEDED run lets the sensor pass (so the next stage launches). A
        terminal non-success state (FAILED / CANCELLED / UNKNOWN) fails this task,
        which stops the pipeline -downstream launch tasks won't run. Still
        running -> keep poking.

        Args:
            tower_run_id (str): The Nextflow Tower run ID to monitor

        Returns:
            bool: True once the workflow has succeeded

        Raises:
            AirflowException: If the workflow reached a terminal non-success state
        """
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(tower_run_id)
        state = workflow.status.state.value
        logger.info(f"Current workflow state: {state}")
        if not workflow.status.is_done:
            return False
        if workflow.status.is_successful:
            return True
        raise AirflowException(
            f"Tower run {tower_run_id} ended in non-success state '{state}'; "
            "stopping the pipeline (downstream stages will not launch)."
        )

    @task()
    def record_provenance(
        synstage_run_id: str,
        sarek_run_id: str,
        synindex_run_id: str,
        **context: Any,
    ) -> None:
        """Attach Synapse Activity provenance to the ComputeTask's output folder.

        Thin wrapper: builds the Synapse + Tower clients from params and delegates
        to record_run_provenance (the testable, Airflow-free implementation).

        Args:
            synstage_run_id (str): The Nextflow Tower run ID of the launched synstage workflow.
            sarek_run_id (str): The Nextflow Tower run ID of the launched nf-core/sarek workflow.
            synindex_run_id (str): The Nextflow Tower run ID of the launched synindex workflow.
        """
        params = context["params"]
        compute_task = ComputeTask.load(params["compute_task_id"])
        syn = SynapseHook(params["synapse_conn_id"]).client
        tower_ops = NextflowTowerHook(params["tower_conn_id"]).ops
        record_run_provenance(
            syn=syn,
            tower_ops=tower_ops,
            compute_task=compute_task,
            synstage_run_id=synstage_run_id,
            sarek_run_id=sarek_run_id,
            synindex_run_id=synindex_run_id,
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


dag = bdf_sarek_somatic_pipeline_poc_dag()


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

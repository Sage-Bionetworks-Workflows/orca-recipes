"""ORCA recipe: nf-core/sarek WES somatic variant calling (tumor vs. matched normal), GRCh38.
Runs on Nextflow Tower and returns results to Synapse.

Orchestrates four steps:
  1. fetch_samplesheet    : Fetch the samplesheet from Synapse to S3
  2. nf-synapse synstage  : Download FASTQ files from Synapse to S3
  3. sarek                : Run nf-core/sarek somatic variant calling
  4. nf-synapse synindex  : Index results back to Synapse

Somatic mode is set by the samplesheet, not a flag: a tumor (status=1) and matched
normal (status=0) sharing a `patient` id trigger tumor-vs-normal calling.

Config reproduces the JHU NF1 Biobank release-2 run (JH_batch1): sarek 3.1.2,
GATK.GRCh38, WES + Agilent V6 intervals, callers strelka,mutect2,vep.
Ref: https://github.com/nf-osi/biobank-release-2

Prerequisites:
  - SYNAPSE_AUTH_TOKEN set as a Tower workspace secret (not a user secret).
  - Airflow connections configured for Synapse, Nextflow Tower, and AWS.

DAG Parameters:
- `synapse_conn_id`: Connection ID for the Synapse service account.
- `tower_conn_id`: Connection ID for the Nextflow Tower workspace.
- `tower_compute_env_type`: Tower compute environment filter to launch runs on.
- `aws_conn_id`: Connection ID used to stage the samplesheet into S3.
- `samplesheet_id`: Synapse ID of the samplesheet to run.
- `samplesheet_name`: File name of the samplesheet.
- `samplesheet_version`: Synapse version to fetch (null = latest).
- `output_folder_id`: Synapse folder where synindex uploads results.
- `bucket_name` / `staging_key`: S3 staging location for the run.
- `institution`: sample-generating institution ('JH' or 'WU'); selects the BED.
- `run_number`: run version (increment for a clean rerun preserving outputs).
"""

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import synapseutils
from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook
from synapseclient import Activity

# BED files for exome seq data from JHU NF1 repository - different batches/institutions
BED_JH = "s3://ntap-add5-project-tower-bucket/reference/Baits_BED_Files_AgilentV6_REVISED_S07604514_ALLBED_merged_020816_withChr_GRCh38_sorted.bed"
BED_WU = "s3://ntap-add5-project-tower-bucket/reference/xgen-exome-research-panel-v2-probes-hg3862a5791532796e2eaa53ff00001c1b3c.bed"

# Optional panel of normals for Mutect2. GATK.GRCh38 igenomes already provides the
# af-only-gnomad germline resource, so PON is optional; leave as None to run without one.
PON = None  # e.g. "s3://.../1000g_pon.hg38.vcf.gz"
PON_TBI = None  # e.g. "s3://.../1000g_pon.hg38.vcf.gz.tbi"


@dataclass
class Dataset:
    """A sarek somatic dataset and the S3/run naming derived from it."""

    id: str
    """The synapse id for the samplesheet."""

    samplesheet: str
    """The name of the samplesheet to run."""

    synapse_id_for_output: str
    """The synapse id for the output folder, where the output will be uploaded to."""

    bucket_name: str
    """The name of the bucket to stage the samplesheet in."""

    staging_key: str
    """The key in the S3 bucket where this workflow is going to run."""

    institution: str
    """The institution that generated the samples ('JH' or 'WU'). Determines the BED file."""

    run_number: int = 1
    """Run version number. Increment to preserve previous outputs."""

    version: Optional[int] = None
    """Synapse version of the samplesheet to fetch. None = latest (default)."""

    @property
    def intervals(self) -> str:
        """The S3 uri for the BED file, determined by institution."""
        if self.institution == "JH":
            return BED_JH
        elif self.institution == "WU":
            return BED_WU
        raise ValueError(f"Unknown institution '{self.institution}'. Expected 'JH' or 'WU'.")

    @property
    def samplesheet_location(self) -> str:
        """The location where the unstaged samplesheet is located."""
        return f"{self.samplesheet_location_prefix}{self.samplesheet}"

    @property
    def samplesheet_to_stage_key(self) -> str:
        """The key in the S3 bucket where the samplesheet is going to be staged."""
        return f"{self.staging_key}to_stage/{self.samplesheet}"

    @property
    def staged_samplesheet_location(self) -> str:
        """The S3 uri where the samplesheet is staged."""
        return f"{self.staging_location}synstage_{self.id}/{self.samplesheet}"

    @property
    def staging_location(self) -> str:
        """The S3 uri where the workflow is going to be run."""
        return f"s3://{self.bucket_name}/{self.staging_key}"

    @property
    def samplesheet_location_prefix(self) -> str:
        """The S3 uri where the unstaged samplesheet is located."""
        return f"s3://{self.bucket_name}/{self.staging_key}to_stage/"

    @property
    def output_directory(self) -> str:
        """The S3 uri where the output is uploaded to; the input for synindex."""
        return f"s3://{self.bucket_name}/outputs/sarek_somatic_GRCh38_{self.id}_{self.run_number}/"

    @property
    def synstage_run_name(self) -> str:
        return f"synstage_{self.id}"

    @property
    def sarek_run_name(self) -> str:
        return f"sarek_somatic_GRCh38_{self.id}_{self.run_number}"

    @property
    def synindex_run_name(self) -> str:
        return f"synindex_{self.id}_{self.run_number}"

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> "Dataset":
        """Build a Dataset from the DAG run params."""
        return cls(
            id=params["samplesheet_id"],
            samplesheet=params["samplesheet_name"],
            synapse_id_for_output=params["output_folder_id"],
            bucket_name=params["bucket_name"],
            staging_key=params["staging_key"],
            institution=params["institution"],
            run_number=params["run_number"],
            version=params.get("samplesheet_version"),
        )


def prepare_synstage_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synapse synstage."""
    return LaunchInfo(
        run_name=dataset.synstage_run_name,
        pipeline="Sage-Bionetworks-Workflows/nf-synapse",
        revision="main",
        profiles=["sage"],
        params={
            "input": dataset.samplesheet_location,
            "outdir": dataset.staging_location,
            "entry": "synstage",
        },
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],  # workspace secret (not user secret)
    )


def prepare_sarek_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for the nf-core/sarek somatic run.

    Reproduces JHU NF1 Biobank release-2 somatic calling: Strelka2 + Mutect2,
    annotated with VEP. https://github.com/nf-osi/biobank-release-2
    """
    params = {
        "input": dataset.staged_samplesheet_location,
        "outdir": dataset.output_directory,
        "wes": True,
        "intervals": dataset.intervals,
        "igenomes_base": "s3://sage-igenomes/igenomes",
        "genome": "GATK.GRCh38",
        "tools": "strelka,mutect2,vep",
    }
    if PON:
        params["pon"] = PON
        params["pon_tbi"] = PON_TBI
    return LaunchInfo(
        run_name=dataset.sarek_run_name,
        pipeline="nf-core/sarek",
        revision="3.1.2",  # matches JHU Biobank release-2 (sarek v3.1.2)
        profiles=["sage"],
        params=params,
    )


def prepare_synindex_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synapse synindex."""
    return LaunchInfo(
        run_name=dataset.synindex_run_name,
        pipeline="Sage-Bionetworks-Workflows/nf-synapse",
        revision="main",
        profiles=["sage"],
        params={
            "s3_prefix": dataset.output_directory,
            "parent_id": dataset.synapse_id_for_output,
            "entry": "synindex",
        },
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],  # workspace secret (not user secret)
    )


def fetch_samplesheet_from_record_set(params: dict[str, Any]) -> None:
    """Alternative to the `fetch_samplesheet` task: materialize the synstage input
    samplesheet from a Synapse RecordSet instead of a curated CSV file.

    This is the intended future source of the samplesheet. Wire it in later by
    replacing the body of the `fetch_samplesheet` task with:

        fetch_samplesheet_from_record_set(context["params"])

    The RecordSet (`params['record_set_id']`) is expected to export the samplesheet
    (columns patient,sample,fastq_1,fastq_2,lane,status; fastqs as syn:// URIs)
    that synstage consumes. It is downloaded to a temp CSV and uploaded to the same
    S3 `to_stage` location the current method uses, so nothing downstream changes.
    """
    import tempfile

    from synapseclient.models import RecordSet

    dataset = Dataset.from_params(params)
    syn = SynapseHook(params["synapse_conn_id"]).client
    with tempfile.TemporaryDirectory() as tmp_dir:
        record_set = RecordSet(id=params["record_set_id"], path=tmp_dir).get(
            synapse_client=syn
        )
        S3Hook(aws_conn_id=params["aws_conn_id"]).load_file(
            filename=record_set.path,
            key=dataset.samplesheet_to_stage_key,
            bucket_name=dataset.bucket_name,
            replace=True,
        )


# task_status value on the RecordSet that indicates it is ready for processing.
RECORD_SET_READY_STATUS = "COMPLETE"


def record_set_ready(params: dict[str, Any]) -> bool:
    """Poll check: is the RecordSet's `task_status` COMPLETE (ready for processing)?

    NOT WIRED INTO THIS DAG YET. Intended to back an Airflow sensor that polls the
    RecordSet every 10 seconds until the upstream compute task marks it complete,
    gating the rest of the pipeline. `mode="poke"` (not "reschedule") is what makes
    a true ~10s cadence possible, since the sensor holds a worker slot and re-checks
    every `poke_interval`. Wire it in later as the first step, e.g.:

        @task.sensor(poke_interval=10, timeout=60 * 60 * 24, mode="poke")
        def wait_for_record_set(**context: Any) -> bool:
            return record_set_ready(context["params"])

        ready = wait_for_record_set()
        ready >> fetch_samplesheet()   # (swapped to the RecordSet source)

    There is no native `task_status` field on a Synapse RecordSet yet, so for now
    this reads a `task_status` *entity annotation* -- which you can set today to
    test, e.g.:

        from synapseclient.models import RecordSet
        rs = RecordSet(id="syn123").get()
        rs.annotations["task_status"] = ["COMPLETE"]
        rs.store()

    Swap the annotation read for the real field once it exists. Returns True once
    the RecordSet's `task_status` equals ``COMPLETE``.
    """
    from synapseclient.models import RecordSet

    syn = SynapseHook(params["synapse_conn_id"]).client
    record_set = RecordSet(id=params["record_set_id"]).get(synapse_client=syn)

    # Annotations are dict[str, list[...]]; take the first value if present.
    annotations = record_set.annotations or {}
    values = annotations.get("task_status") or []
    task_status = values[0] if values else None

    print(f"RecordSet {params['record_set_id']} task_status={task_status}")
    return task_status == RECORD_SET_READY_STATUS


def extract_fastq_synapse_ids(samplesheet_path: str) -> list[str]:
    """Parse the fastq_1/fastq_2 syn:// URIs from a sarek samplesheet into synIDs.

    Used for minimal provenance: the original input FASTQ files each output
    ultimately derives from.
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
    "aws_conn_id": Param("AWS_DNT_DEV_SQS_CONN", type="string"),
    "samplesheet_id": Param("syn52236715", type="string"),
    "samplesheet_name": Param(
        "sarek_JH_batch1_1_reprocess_samplesheet.csv", type="string"
    ),
    "samplesheet_version": Param(None, type=["null", "integer"]),
    # RecordSet exposing the samplesheet; used by the alternative
    # fetch_samplesheet_from_record_set() source (not wired in yet).
    "record_set_id": Param(None, type=["null", "string"]),
    "output_folder_id": Param("TODO-syn-JH_batch1-output-folder", type="string"),
    "bucket_name": Param("ntap-add5-project-tower-bucket", type="string"),
    "staging_key": Param("samplesheets/Sarek_Process/EAGER-somatic/", type="string"),
    "institution": Param("JH", type="string", enum=["JH", "WU"]),
    "run_number": Param(1, type="integer"),
}

dag_config = {
    "schedule": None,
    "start_date": datetime(2026, 7, 15),
    "catchup": False,
    "default_args": {
        "retries": 1,
    },
    "tags": ["nextflow_tower", "synapse", "sarek"],
    "params": dag_params,
}


@dag(**dag_config)
def bdf_sarek_somatic_pipeline_dag() -> DAG:
    @task()
    def fetch_samplesheet(**context: Any) -> None:
        """Download the samplesheet from Synapse and upload it to S3 where synstage reads it.

        Current method: fetch a curated samplesheet CSV file by its synID. To
        source the samplesheet from a Synapse RecordSet instead, replace the body
        below with `fetch_samplesheet_from_record_set(context["params"])`.
        """
        params = context["params"]
        dataset = Dataset.from_params(params)
        syn = SynapseHook(params["synapse_conn_id"]).client
        samplesheet_file = syn.get(dataset.id, version=dataset.version)
        S3Hook(aws_conn_id=params["aws_conn_id"]).load_file(
            filename=samplesheet_file.path,
            key=dataset.samplesheet_to_stage_key,
            bucket_name=dataset.bucket_name,
            replace=True,
        )

    @task()
    def launch_synstage(**context: Any) -> str:
        """Launch nf-synapse synstage to download the samplesheet's FASTQs to S3."""
        dataset = Dataset.from_params(context["params"])
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        return hook.ops.launch_workflow(
            prepare_synstage_info(dataset),
            context["params"]["tower_compute_env_type"],
            ignore_previous_runs=True,
        )

    @task()
    def launch_sarek(**context: Any) -> str:
        """Launch nf-core/sarek somatic variant calling."""
        dataset = Dataset.from_params(context["params"])
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        return hook.ops.launch_workflow(
            prepare_sarek_launch_info(dataset),
            context["params"]["tower_compute_env_type"],
            ignore_previous_runs=True,
        )

    @task()
    def launch_synindex(**context: Any) -> str:
        """Launch nf-synapse synindex to index sarek results back into Synapse."""
        dataset = Dataset.from_params(context["params"])
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        return hook.ops.launch_workflow(
            prepare_synindex_launch_info(dataset),
            context["params"]["tower_compute_env_type"],
            ignore_previous_runs=True,
        )

    @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
    def monitor_workflow(run_id: str, **context: Any) -> bool:
        """Poll Tower until the given run reaches a terminal state."""
        hook = NextflowTowerHook(context["params"]["tower_conn_id"])
        workflow = hook.ops.get_workflow(run_id)
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

        `used` = the input samplesheet synID and the original input FASTQ synIDs;
        the pipeline config and Tower run IDs are recorded in the activity
        description. The activity is set on every file synindex indexed under the
        output folder.
        """
        params = context["params"]
        dataset = Dataset.from_params(params)
        syn = SynapseHook(params["synapse_conn_id"]).client

        samplesheet_file = syn.get(dataset.id, version=dataset.version)
        fastq_ids = extract_fastq_synapse_ids(samplesheet_file.path)
        used = [dataset.id, *fastq_ids]

        activity = Activity(
            name=dataset.sarek_run_name,
            description=(
                "nf-core/sarek 3.1.2 somatic variant calling "
                "(genome=GATK.GRCh38, wes=True, tools=strelka,mutect2,vep, "
                f"intervals={dataset.intervals}, institution={dataset.institution}). "
                f"Input samplesheet: {dataset.id}. "
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
            syn, dataset.synapse_id_for_output
        ):
            for _name, syn_id in filenames:
                stored = syn.setProvenance(syn_id, stored or activity)
                indexed += 1
        print(
            f"Set provenance on {indexed} output file(s) under "
            f"{dataset.synapse_id_for_output}; used={used}"
        )

    fetch = fetch_samplesheet()

    synstage_run_id = launch_synstage()
    synstage_done = monitor_workflow.override(task_id="monitor_synstage")(synstage_run_id)

    sarek_run_id = launch_sarek()
    sarek_done = monitor_workflow.override(task_id="monitor_sarek")(sarek_run_id)

    synindex_run_id = launch_synindex()
    synindex_done = monitor_workflow.override(task_id="monitor_synindex")(synindex_run_id)

    provenance = record_provenance(synstage_run_id, sarek_run_id, synindex_run_id)

    # Strict ordering: fetch -> synstage -> sarek -> synindex -> provenance, each
    # stage waiting for the previous to finish.
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
    dag.test()

"""ORCA recipe: BDF POC nf-core pipeline processing (DAG factory).

Builds one Airflow DAG per dataset defined in bdf_poc_configs.yaml.
Each DAG orchestrates 5 steps and returns results to Synapse:

  1. fetch_samplesheet    : Fetch the samplesheet from Synapse to S3
  2. nf-synapse synstage  : Download FASTQ files from Synapse to S3
  3. pipeline             : Run the configured nf-core pipeline (rnaseq, sarek, ...)
  4. nf-synapse synindex  : Index results back to Synapse
  5. record_provenance    : Attach per-sample Activity provenance to outputs

The analysis stage is pipeline configurable: each config entry names its
pipeline (e.g. nf-core/rnaseq or nf-core/sarek), revision, and an inline
params block of science params (aligner, genome, star_index / intervals, ...).

Per-dataset Synapse/S3/Tower settings also live in bdf_poc_configs.yaml.

Reproduces the NF-OSI Nextflow Data Processing configuration:
https://sagebionetworks.jira.com/wiki/spaces/NPD/pages/2595913729/

Prerequisites:
  - SYNAPSE_AUTH_TOKEN set as a Tower workspace secret
  - Airflow connections configured for Synapse, Nextflow Tower, and AWS
"""

import csv
import io
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from orca.services.nextflowtower import NextflowTowerHook
from orca.services.nextflowtower.models import LaunchInfo
from orca.services.synapse import SynapseHook
from synapseclient import Activity, File

from src.utils import get_logger

logger = get_logger(__name__)

# Per-dataset configs (one DAG each) and the repo root for resolving params paths.
CONFIG_PATH = Path(__file__).parent / "bdf_poc_configs.yaml"

# CurationTask lifecycle state that means the RecordSet (samplesheet) is ready
CURATION_TASK_READY_STATUS = "COMPLETED"


def load_bdf_poc_configs(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load the per-dataset BDF DAG configs from YAML."""
    with open(path) as handle:
        return yaml.safe_load(handle) or {}


def curation_task_status(syn: "synapseclient.Synapse", curation_task_id: str) -> Optional[str]:
    """Retrieve the CurationTask's lifecycle state using
    GET /curation/task/{taskId}/status

    TODO: Update to use CurationTask.get_status() once
        synapseclient >= 4.8 is required.

    Args:
        syn: The Synapse client instance
        curation_task_id: The ID of the CurationTask to check

    Returns:
        str: The CurationTask's state, or None if unavailable.
    """
    status = syn.restGET(f"/curation/task/{curation_task_id}/status")
    return status.get("state")


@dataclass
class Dataset:
    """A dataset and the S3/run naming derived from it."""

    id: str
    """The Synapse ID of the samplesheet."""

    samplesheet: str
    """The samplesheet file name."""

    synapse_id_for_output: str
    """The Synapse folder where synindex uploads results."""

    bucket_name: str
    """The S3 bucket to stage in."""

    staging_key: str
    """The S3 key prefix under which staging + outputs are written."""

    output_prefix: str
    """Short pipeline label used in run names + output S3 keys (e.g. rnaseq_GRCh38)."""

    run_number: int = 1
    """Run version number, increment for a clean rerun preserving prior outputs.
    TODO: Make this a DAG param and/or derive from the RecordSet's version"""

    version: Optional[int] = None
    """Synapse version of the samplesheet to fetch."""

    @property
    def staging_location(self) -> str:
        """The S3 URI of the staging folder (for synstage input + output)
        """
        return f"s3://{self.bucket_name}/{self.staging_key}"

    @property
    def samplesheet_location_prefix(self) -> str:
        """The S3 URI prefix of the unstaged samplesheet (synstage input)."""
        return f"s3://{self.bucket_name}/{self.staging_key}to_stage/"

    @property
    def samplesheet_location(self) -> str:
        """S3 URI of the unstaged samplesheet (synstage input)."""
        return f"{self.samplesheet_location_prefix}{self.samplesheet}"

    @property
    def samplesheet_to_stage_key(self) -> str:
        """S3 key where the samplesheet is uploaded for synstage to process"""
        return f"{self.staging_key}to_stage/{self.samplesheet}"

    @property
    def staged_samplesheet_location(self) -> str:
        """S3 URI of the rewritten samplesheet produced by synstage."""
        return f"{self.staging_location}synstage_{self.id}/{self.samplesheet}"

    @property
    def output_directory(self) -> str:
        """Run-specific S3 URI for outputs, the input for synindex."""
        return f"s3://{self.bucket_name}/outputs/{self.output_prefix}_{self.id}_{self.run_number}/"

    @property
    def synstage_run_name(self) -> str:
        """The run name for the synstage step."""
        return f"synstage_{self.id}"

    @property
    def pipeline_run_name(self) -> str:
        """The run name for the main pipeline step."""
        return f"{self.output_prefix}_{self.id}_{self.run_number}"

    @property
    def synindex_run_name(self) -> str:
        """The run name for the synindex step."""
        return f"synindex_{self.id}_{self.run_number}"

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> "Dataset":
        """Build a Dataset from the DAG run params.
        Args:
            params: The DAG run params dictionary

        Returns:
            Dataset: The constructed Dataset instance
        """
        return cls(
            id=params["samplesheet_id"],
            samplesheet=params["samplesheet_name"],
            synapse_id_for_output=params["output_folder_id"],
            bucket_name=params["bucket_name"],
            staging_key=params["staging_key"],
            output_prefix=params["output_prefix"],
            run_number=params["run_number"],
            version=params.get("samplesheet_version"),
        )


def prepare_synstage_info(dataset: Dataset) -> LaunchInfo:
    """LaunchInfo for nf-synapse synstage.

    Args:
        dataset: The Dataset instance containing the necessary information.

    Returns:
        LaunchInfo: The constructed LaunchInfo instance for the synstage step.
    """
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
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
    )


def prepare_pipeline_launch_info(
    dataset: Dataset,
    pipeline: str,
    revision: str,
    params: dict[str, Any],
    nxf_ver: Optional[str] = None,
    nextflow_config: Optional[str] = None,
) -> LaunchInfo:
    """LaunchInfo for the configured nf-core analysis pipeline (stage 3).

    Takes the inline science params (from the config entry) and adds the
    derived input (staged samplesheet) and outdir (run output directory).

    Args:
        dataset: The Dataset instance containing the necessary information.
        pipeline: The name of the pipeline to launch.
        revision: The revision of the pipeline to use (e.g. branch, tag, or commit)
        params: The dictionary of pipeline parameters.
        nxf_ver: Optional Nextflow version to use.
        nextflow_config: Optional path to a Nextflow configuration file.

    Returns:
        LaunchInfo: The constructed LaunchInfo instance for the pipeline step.
    """
    launch_params = {
        **params,
        "input": dataset.staged_samplesheet_location,
        "outdir": dataset.output_directory,
    }
    return LaunchInfo(
        run_name=dataset.pipeline_run_name,
        pipeline=pipeline,
        revision=revision,
        profiles=["sage"],
        params=launch_params,
        pre_run_script=f"NXF_VER={nxf_ver}" if nxf_ver else None,
        nextflow_config=nextflow_config,
    )


def prepare_synindex_info(dataset: Dataset) -> LaunchInfo:
    """LaunchInfo for nf-synapse synindex.

    Args:
        dataset: The Dataset instance containing the necessary information.

    Returns:
        LaunchInfo: The constructed LaunchInfo instance for the synindex step.
    """
    return LaunchInfo(
        run_name=dataset.synindex_run_name,
        pipeline="Sage-Bionetworks-Workflows/nf-synapse",
        revision="dpe-1746-add-provenance-tracking", # TODO: update to main once merged
        profiles=["sage"],
        params={
            "s3_prefix": dataset.output_directory,
            "parent_id": dataset.synapse_id_for_output,
            "entry": "synindex",
        },
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],  # workspace secret (not user secret)
    )


def _row_fastq_ids(row: dict[str, str]) -> list[str]:
    """Return the ordered, de-duplicated fastq synIDs from a single samplesheet row.

    Args:
        row: A samplesheet row mapping column names to values.

    Returns:
        list[str]: The synapse IDs found in the row's "fastq_1"/"fastq_2" columns.
    """
    fastq_ids: list[str] = []
    for column in ("fastq_1", "fastq_2"):
        for synapse_id in re.findall(r"syn\d+", (row.get(column) or "").strip()):
            if synapse_id not in fastq_ids:
                fastq_ids.append(synapse_id)
    return fastq_ids


def parse_sample_fastq_map(samplesheet_path: str) -> dict[str, list[str]]:
    """Map each samplesheet sample to the union of its fastq_1/fastq_2 synIDs.

       Uses utf-8-sig encoding to handle potential BOM (Byte Order Mark) in the CSV file.
       Assumes the CSV file has columns named "sample", "fastq_1", and "fastq_2".

    Args:
        samplesheet_path: The path to the samplesheet CSV file.

    Returns:
        dict[str, list[str]]: A dictionary mapping each sample to a list of its fastq synapse IDs.
    """
    sample_map: dict[str, list[str]] = {}
    with open(samplesheet_path, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            sample = (row.get("sample") or "").strip()
            if not sample:
                continue
            fastq_ids = sample_map.setdefault(sample, [])
            for synapse_id in _row_fastq_ids(row):
                if synapse_id not in fastq_ids:
                    fastq_ids.append(synapse_id)
    return sample_map


def download_samplesheet(syn: "synapseclient.Synapse", params: dict[str, Any], dest_dir: str) -> str:
    """Download the samplesheet CSV to a local directory

    Precedence:
      1. Checks for override_samplesheet_id in params, if present
            gets that file from synapse
      2. Checks if use_record_set is True in params, if so gets
            the samplesheet from the specified RecordSet.
      3. If neither of the above conditions are met, gets the samplesheet file from
            the specified samplesheet_id and optional version

    Args:
        syn: The Synapse client instance.
        params: The dictionary of parameters controlling the download behavior.
        dest_dir: The destination directory to download the samplesheet to.

    Returns:
        str: The path to the downloaded samplesheet CSV file.
    """
    override_samplesheet_id = params.get("override_samplesheet_id")
    if override_samplesheet_id:
        return syn.get(override_samplesheet_id, downloadLocation=dest_dir).path
    if params["use_record_set"]:
        from synapseclient.models import RecordSet

        record_set = RecordSet(id=params["record_set_id"], path=dest_dir).get(synapse_client=syn)
        return record_set.path
    return syn.get(
        params["samplesheet_id"],
        version=params.get("samplesheet_version"),
        downloadLocation=dest_dir,
    ).path


def match_sample_by_path(object_uri: str, sample_fastqs: dict[str, list[str]]) -> Optional[str]:
    """Return the sample that appears as a folder segment of object_uri.

    Both pipelines put the sample name in the output path, so this works for both:
      - rnaseq: .../star_salmon/<sample>/quant.sf
      - sarek:  .../annotation/strelka/<sample>/<sample>.strelka...vcf.gz
    If several samples match (one a substring of another), the longest wins.

    TODO: This method could fail with different pipeline params like mutect2 for sarek, which has two samples
    in the path (tumor/normal). In that case, we may need to use the input samplesheet
    to map the output to a sample.

    Args:
        object_uri: The URI of the object whose sample is to be matched.
        sample_fastqs: A dictionary mapping sample names to lists of their fastq synapse IDs.

    Returns:
        Optional[str]: The name of the matching sample, or None if no match is found.
    """
    segments = set(object_uri.rstrip("/").split("/"))
    matches = [sample for sample in sample_fastqs if sample and sample in segments]
    return max(matches, key=len) if matches else None


def select_provenance_sample(
    row: dict[str, str], sample_fastqs: dict[str, list[str]], provenance: dict[str, Any]
) -> Optional[str]:
    """Return the sample for a final output row, or None to skip the row.

    The config's provenance block decides which synindex rows are final
    outputs worth provenancing

      - object_uri_contains: object_uri must contain this substring
        (e.g. "annotation" for sarek's annotated variant outputs).
      - file_name: file_name must equal this exactly
        (e.g. "quant.sf" for rnaseq's per-sample quantification).

    The sample is always derived from the output path (match_sample_by_path).

    Args:
        row: A dictionary representing a final-output row from the synindex.
        sample_fastqs: A dictionary mapping sample names to lists of their fastq synapse IDs.
        provenance: A dictionary specifying the provenance rules for selecting final outputs.

    Returns:
        Optional[str]: The name of the matching sample, or None if the row should be skipped.
    """
    object_uri = row.get("object_uri", "")
    object_uri_contains = provenance.get("object_uri_contains")
    if object_uri_contains and object_uri_contains not in object_uri:
        return None
    file_name_filter = provenance.get("file_name")
    if file_name_filter and row.get("file_name", "") != file_name_filter:
        return None
    return match_sample_by_path(object_uri, sample_fastqs)


def fetch_tower_run_config(ops: "towerapi.TowerOps", run_id: str) -> dict[str, Any]:
    """Fetch the actual launch config Tower recorded for a workflow run.

    Args:
        ops: The operations client instance used to interact with Tower.
        run_id: The ID of the workflow run whose launch config is to be fetched.

    Returns:
        dict[str, Any]: A dictionary containing the launch configuration details
        recorded
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


def submitted_run_configs(dataset: Dataset, params: dict[str, Any]) -> dict[str, Any]:
    """Return the per-stage launch configurations that were submitted.

    This function constructs a dictionary containing the launch configurations for each stage
    (synstage, pipeline, synindex) based on the provided dataset and parameters.

    Args:
        dataset: The Dataset instance containing the necessary information.
        params: A dictionary of parameters used to prepare the pipeline launch info.

    Returns:
        dict[str, Any]: A dictionary containing the per-stage launch configurations.
    """

    def as_dict(info: LaunchInfo) -> dict[str, Any]:
        """Convert a LaunchInfo instance to a dictionary.
        Args:
            info: The LaunchInfo instance to be converted.

        Returns:
            dict[str, Any]: A dictionary representation of the LaunchInfo instance.
        """
        return {
            "pipeline": info.pipeline,
            "revision": info.revision,
            "profiles": info.profiles,
            "params": info.params,
            "config_text": info.nextflow_config,
            "pre_run_script": info.pre_run_script,
        }

    return {
        "synstage": as_dict(prepare_synstage_info(dataset)),
        "pipeline": as_dict(
            prepare_pipeline_launch_info(
                dataset,
                params["pipeline"],
                params["revision"],
                params["params"],
                params.get("nxf_ver"),
                params.get("nextflow_config"),
            )
        ),
        "synindex": as_dict(prepare_synindex_info(dataset)),
    }


def upload_run_configs(syn: "synapseclient.Synapse", dataset: Dataset, run_configs: dict[str, Any]) -> str:
    """Upload the per-stage Tower configs as one JSON file in the output folder.

    Args:
        syn: The Synapse client instance used to interact with Synapse.
        dataset: The Dataset instance containing the necessary information.
        run_configs: A dictionary containing the per-stage launch configurations to be uploaded.

    Returns:
        str: The Synapse ID of the uploaded JSON file.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, f"{dataset.pipeline_run_name}_tower_run_configs.json")
        with open(config_path, "w") as handle:
            json.dump(run_configs, handle, indent=2, sort_keys=True)
        return syn.store(File(config_path, parent=dataset.synapse_id_for_output)).id


def synindex_mapping_uri(dataset: Dataset) -> str:
    """Constructs the S3 URI of the object mapping CSV produced by nf-synapse synindex

    Mirrors nf-synapse SYNINDEX publish_dir:
    ${s3_prefix}/synindex/under-${parent_id}/output.csv.

    Args:
        dataset: The Dataset instance containing the necessary information.

    Returns:
        str: The S3 URI of the synindex mapping CSV file.
    """
    return (
        f"{dataset.output_directory.rstrip('/')}"
        f"/synindex/under-{dataset.synapse_id_for_output}/output.csv"
    )


def read_synindex_mapping(s3_hook: S3Hook, mapping_uri: str) -> list[dict[str, str]]:
    """Read synindex output.csv into one row per indexed file.

    Columns: object_uri, synapse_id, parent_id, file_handle_id, content_md5, file_name.

    Args:
        s3_hook: The S3Hook instance used to interact with S3.
        mapping_uri: The S3 URI of the synindex mapping CSV file.

    Returns:
        list[dict[str, str]]: A list of dictionaries representing the rows in the synindex mapping CSV file.
    """
    bucket, key = S3Hook.parse_s3_url(mapping_uri)
    content = s3_hook.read_key(key=key, bucket_name=bucket)
    # newline="" so csv handles line endings itself (S3 content may use \r\n).
    rows = list(csv.DictReader(io.StringIO(content, newline="")))
    if not rows:
        raise ValueError(f"synindex mapping at {mapping_uri} has no rows")
    return rows


def record_run_provenance(
    syn: "synapseclient.Synapse",
    tower_ops: "towerapi.TowerOps",
    dataset: Dataset,
    params: dict[str, Any],
    run_ids: dict[str, str],
    output_rows: list[dict[str, str]],
) -> list[str]:
    """Attach per-sample Activity provenance to the pipeline's output files.

    Merges synindex's output.csv with the input samplesheet: selects final-output
    rows and their sample via the config's provenance rules (see
    select_provenance_sample - e.g. sarek keeps object_uri containing
    "annotation", rnaseq keeps file_name == "quant.sf"; the sample is derived
    from the output path in both cases), then sets an Activity (used = that
    sample's input FASTQ synIDs) on the output file. One Activity per sample,
    reused across its files.

    Per-output-file lineage (which exact FASTQ made which file) needs nf-core
    TODO: lineage tracing, this is only at the sample level

    Args:
        syn: The Synapse client instance used to interact with Synapse.
        tower_ops: The operations client instance used to interact with Tower.
        dataset: The Dataset instance containing the necessary information.
        params: A dictionary of parameters used to prepare the pipeline launch info.
        run_ids: A dictionary mapping stage names to their corresponding Tower run IDs.
        output_rows: A list of dictionaries representing the rows in the synindex mapping CSV file.

    Returns:
        list[str]: A list of Synapse IDs corresponding to the created Activities.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        sample_fastqs = parse_sample_fastq_map(download_samplesheet(syn, params, tmp_dir))

    # Capture the actual per-run Tower configs (fallback to submitted) and upload
    # them as a run artifact referenced in each Activity's description.
    try:
        run_configs = {
            stage: fetch_tower_run_config(tower_ops, run_id) for stage, run_id in run_ids.items()
        }
    except Exception as error:  # noqa: BLE001 - any Tower/API failure -> fallback
        logger.warning(f"Could not fetch run configs from Tower ({error!r}); using submitted configs.")
        run_configs = submitted_run_configs(dataset, params)
    config_synapse_id = upload_run_configs(syn, dataset, run_configs)

    provenance = params.get("provenance")
    if not provenance:
        logger.info("No provenance config set; skipping per-file provenance.")
        return []

    # Diagnostics for "0 output file(s)": shows the selection rules, the samples
    # parsed from the samplesheet, and an example synindex row (its actual
    # object_uri/file_name columns) so a zero match can be traced.
    logger.info(
        f"provenance selection: rules={provenance}, {len(output_rows)} synindex rows, "
        f"samples={sorted(sample_fastqs)}"
    )
    if output_rows:
        logger.info(f"provenance: example synindex row = {output_rows[0]}")

    pipeline_url = f"https://github.com/{params['pipeline']}"
    run_context = (
        f"{params['pipeline']} {params['revision']} run {dataset.pipeline_run_name}. "
        f"Tower run configs: {config_synapse_id}. Tower runs: {run_ids}."
    )
    activity_by_sample: dict[str, Any] = {}
    provenanced: list[str] = []
    for row in output_rows:
        sample = select_provenance_sample(row, sample_fastqs, provenance)
        if sample is None:
            continue
        if sample not in activity_by_sample:
            activity_by_sample[sample] = Activity(
                name=f"{dataset.pipeline_run_name}:{sample}",
                description=f"Sample {sample}. {run_context}",
                used=sample_fastqs[sample],
                executed=[pipeline_url],
            )
        # setProvenance returns the stored Activity; reuse it for the sample's
        # remaining files rather than creating duplicate Activities.
        activity_by_sample[sample] = syn.setProvenance(row["synapse_id"], activity_by_sample[sample])
        provenanced.append(row["synapse_id"])

    logger.info(
        f"Set per-sample provenance on {len(provenanced)} output file(s) across "
        f"{len(activity_by_sample)} sample(s)."
    )
    return provenanced


def create_bdf_dag(name: str, config: dict[str, Any]) -> DAG:
    """Build one BDF nf-core pipeline DAG from a dataset config entry.

    Args:
        name: The name of the DAG to be created.
        config: A dictionary containing the configuration for the DAG.

    Returns:
        DAG: The constructed Airflow DAG instance.
    """
    dag_params = {
        "synapse_conn_id": Param(config["synapse_conn_id"], type="string"),
        "tower_conn_id": Param(config["tower_conn_id"], type="string"),
        "aws_conn_id": Param(config["aws_conn_id"], type="string"),
        # nf-synapse staging/indexing (synstage, synindex) run on spot; the long,
        # interruption-sensitive nf-core pipeline runs on ondemand.
        "tower_spot_compute_env_type": Param(config.get("tower_spot_compute_env_type", "spot"), type="string"),
        "tower_ondemand_compute_env_type": Param(config.get("tower_ondemand_compute_env_type", "ondemand"), type="string"),
        "samplesheet_id": Param(config["samplesheet_id"], type="string"),
        "samplesheet_name": Param(config["samplesheet_name"], type="string"),
        "samplesheet_version": Param(config.get("samplesheet_version"), type=["null", "integer"]),
        # TEST override: synID of a decoy samplesheet File (fastqs of other datasets)
        # to stage for synstage instead of the real inputs; the trigger stays the
        # actual CurationTask/RecordSet. Null = use the real samplesheet.
        "override_samplesheet_id": Param(config.get("override_samplesheet_id"), type=["null", "string"]),
        # RecordSet/CurationTask trigger. use_record_set=True -> wait on the
        # CurationTask status and source the samplesheet from the RecordSet;
        # False -> skip the wait and fetch the samplesheet File by synID directly
        # (for testing before a CurationTask/RecordSet exists).
        "use_record_set": Param(config.get("use_record_set", True), type="boolean"),
        "curation_task_id": Param(config.get("curation_task_id"), type=["null", "string"]),
        "record_set_id": Param(config.get("record_set_id"), type=["null", "string"]),
        "output_folder_id": Param(config["output_folder_id"], type="string"),
        "bucket_name": Param(config["bucket_name"], type="string"),
        "staging_key": Param(config["staging_key"], type="string"),
        "output_prefix": Param(config["output_prefix"], type="string"),
        "pipeline": Param(config["pipeline"], type="string"),
        "revision": Param(config["revision"], type="string"),
        "params": Param(config["params"], type="object"),
        "nxf_ver": Param(config.get("nxf_ver"), type=["null", "string"]),
        "nextflow_config": Param(config.get("nextflow_config"), type=["null", "string"]),
        "run_number": Param(config.get("run_number", 1), type="integer"),
        # How to select final-output rows for per-sample provenance:
        # {object_uri_contains: "annotation"} for sarek, {file_name: "quant.sf"}
        # for rnaseq. The sample is derived from the output path. Null = skip.
        "provenance": Param(config.get("provenance"), type=["null", "object"]),
    }

    dag_config = {
        "dag_id": f"{name}_bdf_dag",
        "schedule": config.get("schedule"),
        "start_date": datetime(2026, 7, 15),
        "catchup": False,
        "default_args": {"retries": config.get("retries", 1)},
        "tags": ["nextflow_tower", "synapse", "bdf", name],
        "params": dag_params,
    }

    @dag(**dag_config)
    def bdf_dag() -> DAG:
        """The main BDF DAG function containing all tasks and their dependencies."""
        @task.sensor(poke_interval=10, timeout=60 * 60 * 12, mode="reschedule")
        def wait_for_record_set(**context: Any) -> bool:
            """Poll the CurationTask every 10s until it is COMPLETED (RecordSet ready).

            When use_record_set is False (direct-samplesheet test mode), there is
            no CurationTask to wait on, so this passes immediately.

            Returns:
                bool: True if the CurationTask is completed or if use_record_set is False,
                        False if the CurationTask is still in progress.
            """
            params = context["params"]
            if not params["use_record_set"]:
                return True
            syn = SynapseHook(params["synapse_conn_id"]).client
            status = curation_task_status(syn, params["curation_task_id"])
            logger.info(f"curation task {params['curation_task_id']} status={status}")
            return status == CURATION_TASK_READY_STATUS

        @task()
        def fetch_samplesheet(**context: Any) -> None:
            """Materialize the samplesheet and upload it to S3 where synstage reads it.

            Sources it from the CurationTask's RecordSet (use_record_set=True) or,
            for testing, from the samplesheet File by synID (use_record_set=False).
            Either way it lands at the same S3 to_stage key.
            """
            params = context["params"]
            dataset = Dataset.from_params(params)
            syn = SynapseHook(params["synapse_conn_id"]).client
            with tempfile.TemporaryDirectory() as tmp_dir:
                samplesheet_path = download_samplesheet(syn, params, tmp_dir)
                S3Hook(aws_conn_id=params["aws_conn_id"]).load_file(
                    filename=samplesheet_path,
                    key=dataset.samplesheet_to_stage_key,
                    bucket_name=dataset.bucket_name,
                    replace=True,
                )

        @task()
        def launch_synstage(**context: Any) -> str:
            """Launch nf-synapse synstage to download the samplesheet's FASTQs to S3.

            Returns:
                str: The Tower run ID of the launched synstage workflow
            """
            dataset = Dataset.from_params(context["params"])
            hook = NextflowTowerHook(context["params"]["tower_conn_id"])
            return hook.ops.launch_workflow(
                prepare_synstage_info(dataset),
                context["params"]["tower_spot_compute_env_type"],
                ignore_previous_runs=True,
            )

        @task()
        def launch_pipeline(**context: Any) -> str:
            """Launch the configured nf-core analysis pipeline (rnaseq, sarek, ...).

            Returns:
                str: The Tower run ID of the launched pipeline workflow.
            """
            params = context["params"]
            dataset = Dataset.from_params(params)
            hook = NextflowTowerHook(params["tower_conn_id"])
            return hook.ops.launch_workflow(
                prepare_pipeline_launch_info(
                    dataset,
                    pipeline=params["pipeline"],
                    revision=params["revision"],
                    params=params["params"],
                    nxf_ver=params.get("nxf_ver"),
                    nextflow_config=params.get("nextflow_config"),
                ),
                params["tower_ondemand_compute_env_type"],
                ignore_previous_runs=True,
            )

        @task()
        def launch_synindex(**context: Any) -> str:
            """Launch nf-synapse synindex to index results back into Synapse.

            Returns:
                str: The Tower run ID of the launched synindex workflow.
            """
            dataset = Dataset.from_params(context["params"])
            hook = NextflowTowerHook(context["params"]["tower_conn_id"])
            return hook.ops.launch_workflow(
                prepare_synindex_info(dataset),
                context["params"]["tower_spot_compute_env_type"],
                ignore_previous_runs=True,
            )

        @task.sensor(poke_interval=300, timeout=604800, mode="reschedule")
        def monitor_workflow(tower_run_id: str, **context: Any) -> bool:
            """Poll Tower until the run succeeds; fail if it ends non-successfully.

            Only a SUCCEEDED run lets the sensor pass (so the next stage launches).
            A terminal non-success state (FAILED / CANCELLED / UNKNOWN) fails this
            task, stopping the pipeline. Still running -> keep poking.

            Args:
                tower_run_id: The Tower run ID of the workflow to monitor.

            Returns:
                bool: True if the workflow succeeded, False if it is still running.
                      Raises an AirflowException if the workflow ended in a non-success state.
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
            pipeline_run_id: str,
            synindex_run_id: str,
            **context: Any,
        ) -> None:
            """Attach per-sample provenance to the pipeline's synindex-ed outputs.

            Thin wrapper: builds the Synapse/Tower/S3 clients, reads synindex's
            output.csv, and delegates to record_run_provenance.

            Args:
                synstage_run_id: The Tower run ID of the synstage workflow.
                pipeline_run_id: The Tower run ID of the main pipeline workflow.
                synindex_run_id: The Tower run ID of the synindex workflow.
            """
            params = context["params"]
            dataset = Dataset.from_params(params)
            syn = SynapseHook(params["synapse_conn_id"]).client
            tower_ops = NextflowTowerHook(params["tower_conn_id"]).ops
            s3_hook = S3Hook(aws_conn_id=params["aws_conn_id"])
            output_rows = read_synindex_mapping(s3_hook, synindex_mapping_uri(dataset))
            record_run_provenance(
                syn,
                tower_ops,
                dataset,
                params,
                {
                    "synstage": synstage_run_id,
                    "pipeline": pipeline_run_id,
                    "synindex": synindex_run_id,
                },
                output_rows,
            )

        ready = wait_for_record_set()
        fetch = fetch_samplesheet()

        synstage_run_id = launch_synstage()
        synstage_done = monitor_workflow.override(task_id="monitor_synstage")(synstage_run_id)

        pipeline_run_id = launch_pipeline()
        pipeline_done = monitor_workflow.override(task_id="monitor_pipeline")(pipeline_run_id)

        synindex_run_id = launch_synindex()
        synindex_done = monitor_workflow.override(task_id="monitor_synindex")(synindex_run_id)

        provenance = record_provenance(
                    synstage_run_id=synstage_run_id,
                    pipeline_run_id=pipeline_run_id,
                    synindex_run_id=synindex_run_id)
        # Strict ordering: wait -> fetch -> synstage -> pipeline -> synindex ->
        # provenance, each stage waiting for the previous to finish.
        ready >> fetch
        fetch >> synstage_run_id
        synstage_done >> pipeline_run_id
        pipeline_done >> synindex_run_id
        synindex_done >> provenance

    return bdf_dag()


# Load configs and generate one DAG per dataset (assigned to module globals so
# Airflow discovers them)
bdf_poc_configs = load_bdf_poc_configs()
for bdf_name, bdf_config in bdf_poc_configs.items():
    globals()[f"{bdf_name}_bdf_dag"] = create_bdf_dag(bdf_name, bdf_config)


if __name__ == "__main__":
    # Run ONE factory-generated DAG locally via dag.test(), e.g.:
    #   python dags/bdf_dag_factory.py ntap_add5_rnaseq
    # Defaults to the first config entry if no name is given.
    import argparse

    from src.utils import validate_required_secrets

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "config_name",
        nargs="?",
        default=next(iter(bdf_poc_configs)),
        choices=list(bdf_poc_configs),
        help="Config key from bdf_poc_configs.yaml (default: the first entry).",
    )
    args = parser.parse_args()

    selected_config = bdf_poc_configs[args.config_name]
    validate_required_secrets(
        connection_ids=[
            selected_config["tower_conn_id"],
            selected_config["synapse_conn_id"],
            selected_config["aws_conn_id"],
        ],
        variable_names=[],
    )
    globals()[f"{args.config_name}_bdf_dag"].test()

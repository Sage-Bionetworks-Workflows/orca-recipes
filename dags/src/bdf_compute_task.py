"""Temporary ComputeTask shim for the BDF sarek somatic pipeline.

A stand-in for the not-yet-implemented Synapse Data Processing Compute Task. It
centralizes the pieces a real ComputeTask is expected to own so the DAG can
migrate to it with minimal churn later. Today each piece is backed by a DAG
param or a RecordSet annotation; migrating = swapping the method bodies to read
from the real ComputeTask instead.

What a real ComputeTask is expected to own (and how it's faked here):

- record_set_id: the input samplesheet RecordSet. A 1:1 task -> recordset
  link. Faked as a hardcoded DAG param for now; future: a task property
- (RecordBasedMetadataTaskProperties.recordSetId) read straight off the task.
- launch info: the synstage / sarek / synindex LaunchInfo specs. Built here
  from params for now; future: the launch info is captured within the Synapse
  compute task, so these methods read it off the task.
- task status: the state of the Synapse CurationTask that holds the input
  RecordSet (task_properties.recordSetId).

"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from orca.services.nextflowtower.models import LaunchInfo

# CurationTask state
CURATION_TASK_READY_STATUS = "COMPLETED"

# BED files for exome seq data from JHU NF1 repository - different batches/institutions.
BED_JH = "s3://ntap-add5-project-tower-bucket/reference/Baits_BED_Files_AgilentV6_REVISED_S07604514_ALLBED_merged_020816_withChr_GRCh38_sorted.bed"
BED_WU = "s3://ntap-add5-project-tower-bucket/reference/xgen-exome-research-panel-v2-probes-hg3862a5791532796e2eaa53ff00001c1b3c.bed"

# Optional panel of normals for Mutect2. GATK.GRCh38 igenomes already provides the
# af-only-gnomad germline resource, so PON is optional; leave as None to run without one.
PON: Optional[str] = None  # e.g. "s3://.../1000g_pon.hg38.vcf.gz"
PON_TBI: Optional[str] = None  # e.g. "s3://.../1000g_pon.hg38.vcf.gz.tbi"


@dataclass
class ComputeTask:
    """Temporary ComputeTask: recordSetId + launch info + task_status in one place.

    Attributes:
        curation_task_id: task_id of the Synapse CurationTask that holds the input
            RecordSet; its status is polled for readiness.
        record_set_id: RecordSet holding the samplesheet (the CurationTask's
            task_properties.recordSetId; hardcoded here for now).
        samplesheet_id: Synapse ID of the samplesheet file (used to fetch the
            samplesheet + for input provenance until fully sourced from the
            RecordSet).
        output_folder_id: Synapse folder where synindex uploads results.
        bucket_name: S3 bucket to stage in.
        staging_key: S3 key prefix for staging + outputs.
        institution: sample-generating institution ('JH' or 'WU'); selects the BED.
        samplesheet_name: File name of the samplesheet.
        run_number: run version (increment for a clean rerun preserving outputs).
        samplesheet_version: Synapse version of the samplesheet (None = latest).
    """

    curation_task_id: str
    record_set_id: str
    samplesheet_id: str
    output_folder_id: str
    bucket_name: str
    staging_key: str
    institution: str
    samplesheet_name: str
    run_number: int = 1
    samplesheet_version: Optional[int] = None

    @classmethod
    def load(cls, compute_task_id: Optional[str] = None) -> "ComputeTask":
        """Load the compute task, which provides all of its own values.

        These values belong to the compute task, not the DAG -- the DAG only says
        *which* task to run (compute_task_id). Today this returns the single
        task we set up in Synapse for the POC (compute_task_id is accepted but
        ignored); the samplesheet_* fields are transitional and fall away once
        the samplesheet is sourced entirely from the RecordSet.

        Future: replace the body with a real lookup, e.g.
        ComputeTask(id=compute_task_id).get(), that reads record_set_id,
        launch info, output_folder_id, bucket_name, staging_key, institution,
        run_number, and task_status straight off the Synapse compute task.

        Args:
            compute_task_id (Optional[str], optional): ID of the compute task to load. Defaults to None.

        Returns:
            ComputeTask: deserialized ComputeTask object with all its values.
        """
        return cls(
            curation_task_id="6954",
            record_set_id="syn76458430",
            samplesheet_id="syn76340211",
            samplesheet_name="jhu_biobank_wes_demo_samplesheet_test_for_airflow.csv",
            samplesheet_version=None,
            output_folder_id="syn76340288",
            bucket_name="ntap-add5-project-tower-bucket",
            staging_key="samplesheets/Sarek_Process/EAGER-somatic/",
            institution="JH",
            run_number=999,
        )


    @property
    def intervals(self) -> str:
        """The S3 uri for the WES capture BED file, determined by institution."""
        if self.institution == "JH":
            return BED_JH
        if self.institution == "WU":
            return BED_WU
        raise ValueError(f"Unknown institution '{self.institution}'. Expected 'JH' or 'WU'.")

    @property
    def staging_location(self) -> str:
        """S3 uri of the staging location."""
        return f"s3://{self.bucket_name}/{self.staging_key}"

    @property
    def samplesheet_location_prefix(self) -> str:
        """S3 uri prefix of the unstaged samplesheet (synstage input).
        """
        return f"s3://{self.bucket_name}/{self.staging_key}to_stage/"

    @property
    def samplesheet_location(self) -> str:
        """S3 uri of the unstaged samplesheet (synstage input)."""
        return f"{self.samplesheet_location_prefix}{self.samplesheet_name}"

    @property
    def samplesheet_to_stage_key(self) -> str:
        """S3 key where the samplesheet is uploaded for synstage."""
        return f"{self.staging_key}to_stage/{self.samplesheet_name}"

    @property
    def staged_samplesheet_location(self) -> str:
        """S3 uri of the rewritten samplesheet produced by synstage."""
        return f"{self.staging_location}synstage_{self.samplesheet_id}/{self.samplesheet_name}"

    @property
    def output_directory(self) -> str:
        """Run-specific S3 uri for outputs; the input for synindex."""
        return f"s3://{self.bucket_name}/outputs/sarek_somatic_GRCh38_{self.samplesheet_id}_{self.run_number}/"

    @property
    def synstage_run_name(self) -> str:
        return f"synstage_{self.samplesheet_id}"

    @property
    def sarek_run_name(self) -> str:
        return f"sarek_somatic_GRCh38_{self.samplesheet_id}_{self.run_number}"

    @property
    def synindex_run_name(self) -> str:
        return f"synindex_{self.samplesheet_id}_{self.run_number}"


    def synstage_launch_info(self) -> LaunchInfo:
        """LaunchInfo for nf-synapse synstage."""
        return LaunchInfo(
            run_name=self.synstage_run_name,
            pipeline="Sage-Bionetworks-Workflows/nf-synapse",
            revision="main",
            profiles=["sage"],
            params={
                "input": self.samplesheet_location,
                "outdir": self.staging_location,
                "entry": "synstage",
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],  # workspace secret (not user secret)
        )

    def sarek_launch_info(self) -> LaunchInfo:
        """LaunchInfo for the nf-core/sarek somatic run.

        Reproduces JHU NF1 Biobank release-2 somatic calling: Strelka2 + Mutect2,
        annotated with VEP. https://github.com/nf-osi/biobank-release-2
        """
        params: dict[str, Any] = {
            "input": self.staged_samplesheet_location,
            "outdir": self.output_directory,
            "wes": True,
            "intervals": self.intervals,
            "igenomes_base": "s3://sage-igenomes/igenomes",
            "genome": "GATK.GRCh38",
            "tools": "strelka,mutect2,vep",
        }
        if PON:
            params["pon"] = PON
            params["pon_tbi"] = PON_TBI
        return LaunchInfo(
            run_name=self.sarek_run_name,
            pipeline="nf-core/sarek",
            revision="3.1.2",  # matches JHU Biobank release-2 (sarek v3.1.2)
            profiles=["sage"],
            params=params,
        )

    def synindex_launch_info(self) -> LaunchInfo:
        """LaunchInfo for nf-synapse synindex."""
        return LaunchInfo(
            run_name=self.synindex_run_name,
            pipeline="Sage-Bionetworks-Workflows/nf-synapse",
            revision="main",
            profiles=["sage"],
            params={
                "s3_prefix": self.output_directory,
                "parent_id": self.output_folder_id,
                "entry": "synindex",
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],  # workspace secret (not user secret)
        )


    def task_status(self, synapse_client: Any) -> Optional[str]:
        """Retrieve the CurationTask's state (its status in the task lifecycle).

        Uses the raw REST endpoint (equivalent to CurationTask.get_status().state)
        so it works regardless of synapseclient version. Returns None if unavailable.

        Args:
            synapse_client (Any): Synapse client used to fetch the task status.

        Returns:
            Optional[str]: The CurationTask's state, or None if unavailable.
        """
        status = synapse_client.restGET(f"/curation/task/{self.curation_task_id}/status")
        return status.get("state")

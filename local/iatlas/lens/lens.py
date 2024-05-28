#!/usr/bin/env python3

"""
LENS workflow metaflow DAG ***This recipe is a WIP***

based on https://github.com/Sage-Bionetworks-Workflows/py-orca/blob/main/demo.py
"""

import asyncio
from dataclasses import dataclass
from pathlib import PurePosixPath

import s3fs
import yaml
from metaflow import FlowSpec, Parameter, step

from orca.services.nextflowtower import LaunchInfo, NextflowTowerOps
from orca.services.synapse import SynapseOps


@dataclass
class LENSDataset:
    """LENS dataset and relevant details.

    Attributes:
        id: Unique dataset identifier.
        samplesheet: Synapse ID for CSV samplesheet to be used for Synstage and LENS.
        output_folder: Synapse ID for Synindex output folder.
    """

    id: str
    samplesheet: str
    output_folder: str

    def get_run_name(self, suffix: str) -> str:
        """Generate run name with given suffix."""
        return f"{self.id}_{suffix}"

    def synstage_info(
        self, samplesheet_uri: str, s3_prefix: str, stage_key: str
    ) -> LaunchInfo:
        """Generate LaunchInfo for nf-synstage."""
        run_name = self.get_run_name("synstage")
        return LaunchInfo(
            run_name=run_name,
            pipeline="Sage-Bionetworks-Workflows/nf-synapse",
            revision="main",
            entry_name="NF_SYNSTAGE",
            params={
                "input": samplesheet_uri,
                "save_strategy": "flat",
                "outdir": f"{s3_prefix}/{stage_key}",
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )

    def lens_info(
        self, samplesheet_uri: str, s3_prefix: str, stage_key: str
    ) -> LaunchInfo:
        """Generate LaunchInfo for LENS."""
        run_name = self.get_run_name("LENS")
        return LaunchInfo(
            run_name=run_name,
            pipeline="https://gitlab.com/landscape-of-effective-neoantigens-software/lens_for_nf_tower",
            revision="sage",
            params={
                "lens_dir": s3_prefix,
                "fq_dir": f"{s3_prefix}/{stage_key}",
                "global_fq_dir": f"{s3_prefix}/{stage_key}",
                "output_dir": f"{s3_prefix}/{stage_key}_outputs",
                "manifest_path": samplesheet_uri,
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )

    def synindex_info(self, s3_prefix: str, stage_key: str) -> LaunchInfo:
        """Generate LaunchInfo for nf-synindex."""
        return LaunchInfo(
            run_name=self.get_run_name("synindex"),
            pipeline="Sage-Bionetworks-Workflows/nf-synapse",
            revision="main",
            entry_name="NF_SYNINDEX",
            params={
                "s3_prefix": f"{s3_prefix}/{stage_key}_outputs",
                "parent_id": self.output_folder,
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )


class TowerLENSFlow(FlowSpec):
    """Flow for running LENS workflow on Nextflow Tower."""

    tower = NextflowTowerOps()
    synapse = SynapseOps()
    s3 = s3fs.S3FileSystem()

    dataset_id = Parameter(
        "dataset_id",
        type=str,
        help="Synapse ID for a YAML file describing the dataset",
    )

    s3_prefix = Parameter(
        "s3_prefix",
        type=str,
        help="S3 prefix for parent S3 bucket where workflow files will be stored.",
    )

    def monitor_workflow(self, workflow_id):
        """Monitor any workflow run (wait until done)."""
        monitor_coro = self.tower.monitor_workflow(workflow_id)
        status = asyncio.run(monitor_coro)
        if not status.is_successful:
            message = f"Workflow did not complete successfully ({status})."
            raise RuntimeError(message)
        return status

    @step
    def start(self):
        """Entry point."""
        self.next(self.load_dataset)

    @step
    def load_dataset(self):
        """Load dataset from Synapse YAML file."""
        with self.synapse.fs.open(self.dataset_id, "r") as fp:
            kwargs = yaml.safe_load(fp)
        self.dataset = LENSDataset(**kwargs)
        self.next(self.transfer_samplesheet_to_s3)

    @step
    def transfer_samplesheet_to_s3(self):
        """Transfer raw samplesheet from Synapse to Tower S3 bucket."""
        self.samplesheet_uri = (
            f"{self.s3_prefix}/{self.dataset.id}/{self.dataset.id}.csv"
        )
        sheet_contents = self.synapse.fs.readtext(self.dataset.samplesheet)
        self.s3.write_text(self.samplesheet_uri, sheet_contents)
        self.next(self.launch_synstage)

    @step
    def launch_synstage(self):
        """Launch nf-synstage to stage Synapse files in samplesheet."""
        launch_info = self.dataset.synstage_info(
            self.samplesheet_uri, self.s3_prefix, self.dataset_id
        )
        self.lens_manifest_uri = f"{self.s3_prefix}/{self.dataset.id}/{launch_info.run_name}/{self.dataset.id}.csv"
        self.synstage_id = self.tower.launch_workflow(launch_info, "spot")
        self.next(self.monitor_synstage)

    @step
    def monitor_synstage(self):
        """Monitor nf-synstage workflow run (wait until done)."""
        self.monitor_workflow(self.synstage_id)
        self.next(self.launch_lens)

    @step
    def launch_lens(self):
        """Launch LENS workflow."""
        launch_info = self.dataset.lens_info(
            self.lens_manifest_uri, self.s3_prefix, self.dataset_id
        )
        self.LENS_id = self.tower.launch_workflow(launch_info, "spot")
        self.next(self.monitor_lens)

    @step
    def monitor_lens(self):
        """Monitor LENS workflow run (wait until done)."""
        self.monitor_workflow(self.LENS_id)
        self.next(self.launch_synindex)

    @step
    def launch_synindex(self):
        """Launch nf-synindex to index S3 files back into Synapse."""
        launch_info = self.dataset.synindex_info(self.s3_prefix, self.dataset_id)
        self.synindex_id = self.tower.launch_workflow(launch_info, "spot")
        self.next(self.monitor_synindex)

    @step
    def monitor_synindex(self):
        """Monitor nf-synindex workflow run (wait until done)."""
        self.monitor_workflow(self.synindex_id)
        self.next(self.end)

    @step
    def end(self):
        """End point."""
        print(f"Completed processing {self.dataset}")
        print(f"synstage workflow ID: {self.synstage_id}")
        print(f"LENS workflow ID: {self.LENS_id}")
        print(f"synindex workflow ID: {self.synindex_id}")


if __name__ == "__main__":
    TowerLENSFlow()

# run with: python3 local/iatlas/lens/lens.py run \
# --dataset_id syn58366876 \
# --s3_prefix s3://iatlas-project-tower-bucket/LENS \

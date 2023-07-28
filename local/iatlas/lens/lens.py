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
        samplesheet: Synapse ID for nf-core/rnaseq CSV samplesheet.
    """

    id: str
    samplesheet: str

    def get_run_name(self, suffix: str) -> str:
        """Generate run name with given suffix."""
        return f"{self.id}_{suffix}"

    def synstage_info(self, samplesheet_uri: str) -> LaunchInfo:
        """Generate LaunchInfo for nf-synstage."""
        run_name = self.get_run_name("synstage")
        return LaunchInfo(
            run_name=run_name,
            pipeline="Sage-Bionetworks-Workflows/nf-synstage",
            revision="main",
            profiles=["sage"],
            params={
                "input": samplesheet_uri,
                "outdir": "s3://iatlas-project-tower-bucket/LENS/synstage",  # manually load to persistent bucket for now
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        )


class TowerLENSFlow(FlowSpec):
    """Flow for running LENS workflow on Nextflow Tower."""

    tower = NextflowTowerOps()
    synapse = SynapseOps()
    s3 = s3fs.S3FileSystem()

    # Parameters
    dataset_id = Parameter(
        "dataset_id",
        type=str,
        help="Synapse ID for a YAML file describing an RNA-seq dataset",
    )

    s3_prefix = Parameter(
        "s3_prefix",
        type=str,
        help="S3 prefix for storing output files from different runs",
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
        self.next(self.get_lens_outdir)

    @step
    def get_lens_outdir(self):
        """Generate output directory for LENS."""
        run_name = self.dataset.get_run_name("LENS")
        self.lens_outdir = f"{self.s3_prefix}/{run_name}"
        self.next(self.transfer_samplesheet_to_s3)

    @step
    def transfer_samplesheet_to_s3(self):
        """Transfer raw samplesheet from Synapse to Tower S3 bucket."""
        self.samplesheet_uri = f"{self.s3_prefix}/{self.dataset.id}.csv"
        sheet_contents = self.synapse.fs.readtext(self.dataset.samplesheet)
        self.s3.write_text(self.samplesheet_uri, sheet_contents)
        self.next(self.launch_synstage)

    @step
    def launch_synstage(self):
        """Launch nf-synstage to stage Synapse files in samplesheet."""
        launch_info = self.dataset.synstage_info(self.samplesheet_uri)
        self.synstage_id = self.tower.launch_workflow(launch_info, "spot")
        self.next(self.monitor_synstage)

    @step
    def monitor_synstage(self):
        """Monitor nf-synstage workflow run (wait until done)."""
        self.monitor_workflow(self.synstage_id)
        self.next(self.end)

    @step
    def end(self):
        """End point."""
        print(f"Completed processing {self.dataset}")
        print(f"synstage workflow ID: {self.synstage_id}")


if __name__ == "__main__":
    TowerLENSFlow()

# run with: python3 local/lens.py run --dataset_id syn51753796 --s3_prefix s3://iatlas-project-tower-bucket/LENS

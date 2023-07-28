#!/usr/bin/env python3

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

import s3fs
from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo

from utils import configure_logging, monitor_run

SAMPLESHEETS_DIR = "s3://ntap-add5-project-tower-bucket/samplesheets/IBCDPE-528"


async def main():
    ops = NextflowTowerOps()
    datasets = generate_datasets()
    runs = [run_workflows(ops, dataset) for dataset in datasets]
    statuses = await asyncio.gather(*runs)
    print(statuses)


@dataclass
class Dataset:
    id: str
    samplesheet: str

    def get_run_name(self, prefix: str):
        return f"{prefix}_{self.id}"


def upload_input(input_path: str):
    s3 = s3fs.S3FileSystem()
    base_name = os.path.basename(input_path)
    target_uri = f"{SAMPLESHEETS_DIR}/{base_name}"
    s3.upload(input_path, target_uri)
    return target_uri


def generate_datasets():
    """Generate datasets objects from YAML file."""
    input_path = Path("workflows-534.csv").as_posix()
    return [Dataset("syn11638893_updated", upload_input(input_path))]


def prepare_vcf2maf_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-vcf2maf workflow run."""
    nextflow_config = """
    aws.client.uploadMaxThreads = 4
    aws.batch.maxParallelTransfers = 1
    aws.batch.maxTransferAttempts = 5
    aws.batch.delayBetweenAttempts = '180 sec'

    process {
        withName: 'SYNAPSE_(GET|STORE)' { container = "quay.io/brunograndephd/synapsepythonclient:v2.6.0" }
        withName: 'VCF2MAF|EXTRACT_TAR_GZ' { container = "quay.io/brunograndephd/vcf2maf:107.2" }
        withName: '(FILTER|MERGE)_MAFS?' { container = "quay.io/brunograndephd/python:3.10.4" }
    }
    """

    return LaunchInfo(
        run_name=dataset.get_run_name("vcf2maf"),
        pipeline="Sage-Bionetworks-Workflows/nf-vcf2maf",
        revision="1.0.1",
        params={
            "input": dataset.samplesheet,
        },
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        nextflow_config=dedent(nextflow_config),
    )


async def run_workflows(ops: NextflowTowerOps, dataset: Dataset):
    vcf2maf_info = prepare_vcf2maf_launch_info(dataset)
    vcf2maf_run_id = ops.launch_workflow(vcf2maf_info, "spot")
    await monitor_run(ops, vcf2maf_run_id)


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())

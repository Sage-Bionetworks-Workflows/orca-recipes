#!/usr/bin/env python3

import asyncio
from dataclasses import dataclass
from pathlib import Path

from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo

from utils import monitor_run


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
    parent_id: str

    @property
    def rnaseq_run_name(self):
        return f"rnaseq_{self.id}"

    @property
    def synindex_run_name(self):
        return f"synindex_{self.id}"


def generate_datasets() -> list[Dataset]:
    """Generate list of datasets.

    Source: https://sagebionetworks.jira.com/browse/WORKFLOWS-528?focusedCommentId=171979
    """
    prefix = "s3://ntap-add5-project-tower-bucket/samplesheets/IBCDPE-528/synstage"
    return [
        Dataset("syn51198953", f"{prefix}/syn51198953_RNA_Seq_SampleSheet.fixed.csv", "syn51476538"),
        Dataset("syn51198956", f"{prefix}/syn51198956_RNA_Seq_SampleSheet.csv", "syn51476543"),
        Dataset("syn51199003", f"{prefix}/syn51199003_RNA_Seq_SampleSheet.csv", "syn51476548"),
        Dataset("syn51199006", f"{prefix}/syn51199006_RNA_Seq_SampleSheet.csv", "syn51476553"),
    ]


def prepare_rnaseq_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/rnaseq workflow run."""
    return LaunchInfo(
        run_name=dataset.rnaseq_run_name,
        pipeline="nf-core/rnaseq",
        revision="3.11.2",
        profiles=["sage"],
        params={
            "input": dataset.samplesheet,
            "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{dataset.rnaseq_run_name}/",
            "gencode": True,
            "fasta": "s3://sage-igenomes/nf-core/rnaseq-3.7/grch38-gencode40/GRCh38.primary_assembly.genome.fa",
            "transcript_fasta": "s3://sage-igenomes/nf-core/rnaseq-3.7/grch38-gencode40/genome.transcripts.fa",
            "gtf": "s3://sage-igenomes/nf-core/rnaseq-3.7/grch38-gencode40/gencode.v40.primary_assembly.annotation.gtf",
            "gene_bed": "s3://sage-igenomes/nf-core/rnaseq-3.7/grch38-gencode40/gencode.v40.primary_assembly.annotation.bed",
            "star_index": "s3://sage-igenomes/nf-core/rnaseq-3.7/grch38-gencode40/index/star/",
            "salmon_index": "s3://sage-igenomes/nf-core/rnaseq-3.7/grch38-gencode40/index/salmon/",
            "rsem_index": "s3://sage-igenomes/nf-core/rnaseq-3.7/grch38-gencode40/rsem/",
        },
        nextflow_config=Path("nextflow-rnaseq.config").read_text(),
    )


def prepare_synindex_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synindex workflow run."""
    return LaunchInfo(
        run_name=dataset.synindex_run_name,
        pipeline="Sage-Bionetworks-Workflows/nf-synindex",
        revision="main",
        profiles=["sage"],
        params={
            "s3_prefix": f"s3://ntap-add5-project-tower-bucket/outputs/{dataset.rnaseq_run_name}/",
            "parent_id": dataset.parent_id,
        },
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
    )


async def run_workflows(ops: NextflowTowerOps, dataset: Dataset):
    rnaseq_info = prepare_rnaseq_launch_info(dataset)
    rnaseq_run_id = ops.launch_workflow(rnaseq_info, "spot")
    await monitor_run(ops, rnaseq_run_id)

    synindex_info = prepare_synindex_launch_info(dataset)
    synindex_run_id = ops.launch_workflow(synindex_info, "spot")
    await monitor_run(ops, synindex_run_id)


if __name__ == "__main__":
    asyncio.run(main())

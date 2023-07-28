#!/usr/bin/env python3

import asyncio
import csv
from dataclasses import dataclass, replace
from textwrap import dedent

import s3fs
from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo
from yaml import safe_load

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
    starting_step: str
    samplesheet: str
    parent_id: str
    mapping_parent_id: str | None = None

    def get_run_name(self, prefix: str):
        return f"{prefix}_{self.id}"


def generate_datasets():
    """Generate datasets objects from YAML file."""
    with open("workflows-467.yaml", "r") as fp:
        datasets_raw = safe_load(fp)
    return [Dataset(**kwargs) for kwargs in datasets_raw]


def prepare_sarek_v2_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/sarek v2 workflow run."""
    run_name = dataset.get_run_name("sarek_v2")

    params = {
        "input": dataset.samplesheet,
        "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{run_name}/",
        "step": dataset.starting_step,
        "tools": "DeepVariant,FreeBayes,Mutect2,Strelka",
        "model_type": "WGS",
        "genome": "GRCh38",
        "igenomes_base": "s3://sage-igenomes/igenomes",
    }

    nextflow_config = """
    process {
        withLabel:deepvariant {
            cpus = 24
        }
    }
    """

    return LaunchInfo(
        run_name=run_name,
        pipeline="Sage-Bionetworks-Workflows/sarek",
        revision="d48de18fc5342c02c64c0e0b0e704012b28c91dd",
        profiles=["sage"],
        params=params,
        nextflow_config=dedent(nextflow_config),
    )


def prepare_sarek_v3_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/sarek v3 workflow run."""
    run_name = dataset.get_run_name("sarek_v3")

    params = {
        "input": dataset.samplesheet,
        "step": dataset.starting_step,
        "wes": False,
        "igenomes_base": "s3://sage-igenomes/igenomes",
        "genome": "GATK.GRCh38",
        "tools": "cnvkit",
        "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{run_name}/",
    }

    return LaunchInfo(
        run_name=run_name,
        pipeline="Sage-Bionetworks-Workflows/sarek",
        revision="3.1.2-safe",
        profiles=["sage"],
        params=params,
    )


def prepare_synindex_launch_info(dataset: Dataset, launch_info: LaunchInfo) -> LaunchInfo:
    """Generate LaunchInfo for nf-synindex workflow run."""

    return LaunchInfo(
        run_name=dataset.get_run_name("synindex"),
        pipeline="Sage-Bionetworks-Workflows/nf-synindex",
        revision="main",
        profiles=["sage"],
        params={
            "s3_prefix": launch_info.params["outdir"],
            "parent_id": dataset.parent_id,
        },
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
    )


def convert_sarek_v2_to_v3_samplesheet(v2_launch_info: LaunchInfo):
    s3 = s3fs.S3FileSystem()
    outdir = v2_launch_info.params["outdir"].rstrip("/")
    recalibrated_uri = f"{outdir}/Preprocessing/TSV/recalibrated.tsv"

    with s3.open(recalibrated_uri, "r", newline="") as tsvfile:
        reader = csv.reader(tsvfile, delimiter="\t")
        recalibrated_rows = list(reader)

    v3_samplesheet_uri = recalibrated_uri.replace("recalibrated.tsv", "sarek_v3.csv")
    with s3.open(v3_samplesheet_uri, "w", newline="") as csvfile:
        header = ["patient", "sex", "status", "sample", "bam", "bai"]
        writer = csv.writer(csvfile, delimiter=",")
        writer.writerow(header)
        writer.writerows(recalibrated_rows)

    return v3_samplesheet_uri


async def run_workflows(ops: NextflowTowerOps, dataset: Dataset):
    if dataset.starting_step == "mapping":
        sarek_info = prepare_sarek_v2_launch_info(dataset)
    elif dataset.starting_step == "variant_calling":
        sarek_info = prepare_sarek_v3_launch_info(dataset)
    else:
        raise ValueError("Unexpected starting step")
    sarek_run_id = ops.launch_workflow(sarek_info, "spot")
    await monitor_run(ops, sarek_run_id)

    # Index Sarek v2 results in a separate Synapse folder
    if dataset.starting_step == "mapping":
        mapping_dataset = replace(dataset, id=f"sarek_v2_{dataset.id}", parent_id=dataset.mapping_parent_id)
        mapping_synindex_info = prepare_synindex_launch_info(mapping_dataset, sarek_info)
        mapping_synindex_run_id = ops.launch_workflow(mapping_synindex_info, "spot")
        await monitor_run(ops, mapping_synindex_run_id)

    if dataset.starting_step == "mapping":
        sarek_v2_info, dataset_v2 = sarek_info, dataset
        sarek_v3_samplesheet = convert_sarek_v2_to_v3_samplesheet(sarek_v2_info)
        dataset = replace(dataset_v2, samplesheet=sarek_v3_samplesheet, starting_step="variant_calling")
        sarek_info = prepare_sarek_v3_launch_info(dataset)
        sarek_run_id = ops.launch_workflow(sarek_info, "spot")
        await monitor_run(ops, sarek_run_id)

    synindex_info = prepare_synindex_launch_info(dataset, sarek_info)
    synindex_run_id = ops.launch_workflow(synindex_info, "spot")
    await monitor_run(ops, synindex_run_id)


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())

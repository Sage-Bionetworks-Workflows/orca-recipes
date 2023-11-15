"""https://sagebionetworks.jira.com/browse/WORKFLOWS-538
This workflow was created to run the `nf-core/rnaseq` pipeline for GRCm38 RNASeq data.
"""
import asyncio
from dataclasses import dataclass
from pathlib import Path

from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo


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
    samplesheet_location_prefix: str
    staging_location: str
    synapse_id_for_output: str
    run_number: int
    output_location: str
    
    @property
    def samplesheet_location(self) -> str:
        return f"{self.samplesheet_location_prefix}/{self.samplesheet}"
    
    @property
    def staged_samplesheet_location(self) -> str:
        return f"{self.staging_location}synstage_{self.id}/{self.samplesheet}"

    @property
    def synstage_run_name(self) -> str:
        return f"synstage_{self.id}"

    @property
    def rnaseq_run_name(self) -> str:
        return f"rnaseq_GRCm38_{self.id}_{self.run_number}"

    @property
    def synindex_run_name(self) -> str:
        return f"synindex_{self.id}_{self.run_number}"


def generate_datasets() -> list[Dataset]:
    """Generate list of datasets.

    Source: https://sagebionetworks.jira.com/browse/WORKFLOWS-538

    This is expecting that the SampleSheet to be run exists in the `prefix` location.
    """
    return [
        Dataset(
            id="syn51407805",
            samplesheet_location_prefix="s3://ntap-add5-project-tower-bucket/samplesheets/WORKFLOWS-538/to_stage/",
            samplesheet=f"syn51199003_RNA_Seq_SampleSheet.csv",
            staging_location="s3://ntap-add5-project-tower-bucket/samplesheets/Reprocess/WORKFLOWS-538/",
            synapse_id_for_output ="syn52913106",
            run_number=6,
            output_location="s3://ntap-add5-project-tower-bucket/outputs/rnaseq_GRCm38_syn51407805_6/",
        )
    ]


def prepare_synstage_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synstage."""
    return LaunchInfo(
        run_name=dataset.synstage_run_name,
        pipeline="Sage-Bionetworks-Workflows/nf-synstage",
        revision="disable_wave",
        profiles=["sage"],
        params={
            "input": dataset.samplesheet_location,
            "outdir": dataset.staging_location,
        },
        pre_run_script="NXF_VER=22.10.4",
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        nextflow_config="wave.enabled=false"
    )


def prepare_rnaseq_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/rnaseq workflow run."""
    return LaunchInfo(
        run_name=dataset.rnaseq_run_name,
        pipeline="nf-core/rnaseq",
        revision="3.11.2",
        profiles=["sage", "docker"],
        params={
            "input": dataset.staged_samplesheet_location,
            "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{dataset.rnaseq_run_name}/",
            
            "gencode": True,
            "fasta": "s3://sage-igenomes/igenomes/Mus_musculus/Ensembl/GRCm38/Sequence/WholeGenomeFasta/genome.fa",
            "gtf": "s3://sage-igenomes/igenomes/Mus_musculus/Ensembl/GRCm38/Annotation/Genes/genes.gtf",
            "gene_bed": "s3://sage-igenomes/igenomes/Mus_musculus/Ensembl/GRCm38/Annotation/Genes/genes.bed",
            "star_index": "s3://sage-igenomes/igenomes/Mus_musculus/Ensembl/GRCm38/Sequence/STARIndex/",
        },
        pre_run_script="NXF_VER=23.10.0",
        nextflow_config=Path("local/ntap/nextflow-rnaseq.config").read_text(),
    )


def prepare_synindex_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synindex workflow run."""
    return LaunchInfo(
        run_name=dataset.synindex_run_name,
        pipeline="Sage-Bionetworks-Workflows/nf-synindex",
        revision="disable_wave",
        profiles=["sage"],
        params={
            "s3_prefix": dataset.output_location,
            "parent_id": dataset.synapse_id_for_output,
        },
        pre_run_script="NXF_VER=22.10.4",
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        nextflow_config="wave.enabled=false"
    )


async def run_workflows(ops: NextflowTowerOps, dataset: Dataset):
    synstage_info = prepare_synstage_info(dataset)
    synstage_run_id = ops.launch_workflow(synstage_info, "spot")
    status = await ops.monitor_workflow(run_id=synstage_run_id, wait_time=60 * 2)
    print(status)

    scrnaseq_info = prepare_rnaseq_launch_info(dataset)
    scrnaseq_run_id = ops.launch_workflow(scrnaseq_info, "spot")
    status = await ops.monitor_workflow(run_id=scrnaseq_run_id, wait_time=60 * 2)
    print(status)

    synindex_info = prepare_synindex_launch_info(dataset)
    synindex_run_id = ops.launch_workflow(synindex_info, "spot")
    status = await ops.monitor_workflow(run_id=synindex_run_id, wait_time=60 * 2)
    print(status)


if __name__ == "__main__":
    asyncio.run(main())

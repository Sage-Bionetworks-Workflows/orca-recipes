"""https://sagebionetworks.jira.com/browse/WORKFLOWS-560
This workflow was created to run the `nf-core/rnaseq` pipeline for GRCh38 RNASeq data.
"""
import asyncio
import boto3

from synapseclient import Synapse
from dataclasses import dataclass
from pathlib import Path

from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo

session = boto3.Session(profile_name="TowerProd_Administrator")
s3 = session.client("s3")

async def main():
    ops = NextflowTowerOps()
    datasets = generate_datasets()
    runs = [run_workflows(ops, dataset) for dataset in datasets]
    statuses = await asyncio.gather(*runs)
    print(statuses)


@dataclass
class Dataset:
    id: str
    """The synapse id for the samplesheet."""

    samplesheet: str
    """The name of the samplesheet to run."""

    synapse_id_for_output: str
    """The synapse id for the output folder, this is where the output will be uploaded to."""

    run_number: int
    """The number for the run, this is used to generate the run_name for the workflow."""

    output_number: int
    """The number for the output, in some cases this is the same as the run_number, 
    but in other cases it isn't because the pipeline has been re-ran."""

    bucket_name: str
    """The name of the bucket to stage the samplesheet in."""

    staging_key: str
    """The key in the S3 bucket where this workflow is going to run."""

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
        """The S3 uri where the output is going to be uploaded to. The is used as the
        input for the synindex workflow."""
        return f"s3://{self.bucket_name}/outputs/rnaseq_GRCh38_{self.id}_{self.output_number}/"

    @property
    def synstage_run_name(self) -> str:
        """The name of the synstage run."""
        return f"synstage_{self.id}"

    @property
    def rnaseq_run_name(self) -> str:
        """The name of the rnaseq run."""
        return f"rnaseq_GRCh38_{self.id}_{self.run_number}"

    @property
    def synindex_run_name(self) -> str:
        """The name of the synindex run."""
        return f"synindex_{self.id}_{self.run_number}"


def generate_datasets() -> list[Dataset]:
    """Generate list of datasets.

    Source: https://sagebionetworks.jira.com/browse/WORKFLOWS-538

    This is expecting that the SampleSheet to be run exists in the `prefix` location.
    """
    return [
        Dataset(
            id="syn53972620",
            samplesheet="syn51198895_RNA_Seq_SampleSheet.csv",
            staging_key="samplesheets/RNASeq_Process/WORKFLOWS-560/",
            bucket_name="ntap-add5-project-tower-bucket",
            synapse_id_for_output="syn55228511",
            run_number=1,
            output_number=1,
        )
    ]

def stage_samplesheet(syn: Synapse, dataset: Dataset):
    """Download the samplesheet from synapse and upload it to S3 in the location where synstage
    is going to grab the file.

    :param syn: The logged in synapse instance.
    :param dataset: The dataset to stage the samplesheet for.
    """
    samplesheet_file = syn.get(dataset.id)
    samplesheet_file_path = samplesheet_file.path

    s3.upload_file(
        samplesheet_file_path, dataset.bucket_name, dataset.samplesheet_to_stage_key
    )


def prepare_synstage_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synstage."""
    return LaunchInfo(
            run_name=dataset.synstage_run_name,
            pipeline="Sage-Bionetworks-Workflows/nf-synapse",
            revision="main",
            profiles=["sage"],
            entry_name="NF_SYNSTAGE",
            params={
                "input": dataset.samplesheet_location,
                "outdir": dataset.staging_location,
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"]
        )
    
def prepare_rnaseq_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/rnaseq workflow run."""
    return LaunchInfo(
        run_name=dataset.rnaseq_run_name,
        pipeline="nf-core/rnaseq",
        revision="3.11.2",
        profiles=["sage"],
        params={
            "input": dataset.staged_samplesheet_location,
            "outdir": dataset.output_directory,
            "gencode": True,
            "fasta": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Sequence/WholeGenomeFasta/genome.fa",
            "gtf": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Annotation/Genes/genes.gtf",
            "gene_bed": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Annotation/Genes/genes.bed",
            "star_index": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Sequence/STARIndex/",
            # TODO: These could not be found in `s3://sage-igenomes/igenomes/Homo_sapiens/`
            # "transcript_fasta": "",
            # "salmon_index": "",
            # "rsem_index": "",
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
            "s3_prefix": dataset.output_directory,
            "parent_id": dataset.synapse_id_for_output,
        },
        pre_run_script="NXF_VER=22.10.4",
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        nextflow_config="wave.enabled=false",
    )


async def run_workflows(ops: NextflowTowerOps, dataset: Dataset):
    syn = Synapse()
    syn.login()

    # upload samplesheet to S3
    stage_samplesheet(syn, dataset)
    # stage fastq and updated samplesheet
    synstage_info = prepare_synstage_info(dataset)
    synstage_run_id = ops.launch_workflow(synstage_info, "spot")
    status = await ops.monitor_workflow(run_id=synstage_run_id, wait_time=60 * 2)
    print(status)
    
    # run rnaseq pipeline
    rnaseq_info = prepare_rnaseq_launch_info(dataset)
    rnaseq_run_id = ops.launch_workflow(rnaseq_info, "spot")
    status = await ops.monitor_workflow(run_id=rnaseq_run_id, wait_time=60 * 2)
    print(status)

    # index the output files in Synapse
    synindex_info = prepare_synindex_launch_info(dataset)
    synindex_run_id = ops.launch_workflow(synindex_info, "spot")
    status = await ops.monitor_workflow(run_id=synindex_run_id, wait_time=60 * 2)
    print(status)


if __name__ == "__main__":
    asyncio.run(main())

"""https://sagebionetworks.jira.com/browse/WORKFLOWS-555
This workflow was created to run the `nf-core/sarek` pipeline for GRCh38 Whole Genome Sequencing data.
"""
import asyncio
from dataclasses import dataclass

import boto3
from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo
from synapseclient import Synapse

session = boto3.Session(profile_name="TowerProd_Administrator")
s3 = session.client("s3")
import argparse

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('step', nargs='*', default = 'all',help='Processing step (Default: all)')
    args = parser.parse_args()
    
    ops = NextflowTowerOps()
    datasets = generate_datasets()
    runs = [run_workflows(ops, dataset, args.step) for dataset in datasets]
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
        return f"{self.staging_location}synstage_{self.id}/subset_{self.samplesheet}"

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
        return f"s3://{self.bucket_name}/outputs/sarek_GRCh38_{self.id}_{self.output_number}/"

    @property
    def synstage_run_name(self) -> str:
        """The name of the synstage run."""
        return f"synstage_{self.id}"

    @property
    def sarek_run_name(self) -> str:
        """The name of the sarek run."""
        return f"sarek_GRCh38_{self.id}_{self.run_number}"

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
            id="syn55224473",
            samplesheet="syn23664726_Batch2_WGS_SampleSheet.csv",
            staging_key="samplesheets/Sarek_Process/WORKFLOWS-555/",
            bucket_name="ntap-add5-project-tower-bucket",
            synapse_id_for_output="syn55224476",
            run_number=2,
            output_number=1,
        )
    ]

def stage_samplesheet(syn: Synapse, dataset: Dataset) -> None:
    """Download the samplesheet from synapse and upload it to S3 in the location where synstage
    is going to grab the file.
    
    Arguments:
        syn: The logged in synapse instance
        dataset: The dataset to stage the samplesheet for
    """
    samplesheet_file = syn.get(dataset.id)
    samplesheet_file_path = samplesheet_file.path

    s3.upload_file(
        samplesheet_file_path, dataset.bucket_name, dataset.samplesheet_to_stage_key
    )


def prepare_synstage_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synstage.
    
    Arguments:
        dataset: The dataset to stage the samplesheet for
        
    Returns:
        The Nextflow Tower workflow launch specification for synstage step
    """
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
    
def prepare_sarek_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/sarek workflow run.

    Arguments:
        dataset: The dataset to stage the samplesheet for

    Returns:
        The Nextflow Tower workflow launch specification for sarek processing step    
    """
    return LaunchInfo(
        run_name=dataset.sarek_run_name,
        pipeline="nf-core/sarek",
        revision="3.2.2",
        profiles=["sage"],
        params={
            "input": dataset.staged_samplesheet_location,
            "outdir": dataset.output_directory,
            "wes": False,
            "igenomes_base": "s3://sage-igenomes/igenomes",
            "genome": "GATK.GRCh38",
            "tools": "strelka",
        }
    )


def prepare_synindex_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synindex workflow run.
    
    Arguments:
        dataset: The dataset to stage the samplesheet for

    Returns:
        The Nextflow Tower workflow launch specification for synindex step    
    """
    return LaunchInfo(
            run_name=dataset.synindex_run_name,
            pipeline="Sage-Bionetworks-Workflows/nf-synapse",
            revision="main",
            profiles=["sage"],
            entry_name="NF_SYNINDEX",
            params={
                "s3_prefix": dataset.output_directory,
                "parent_id": dataset.synapse_id_for_output,
            },
            workspace_secrets=["SYNAPSE_AUTH_TOKEN"]
        )

async def run_workflows(ops: NextflowTowerOps, dataset: Dataset,step):
    if 'all' in step or 'stage_samplesheet' in step:
        print('staging samplesheet')
        syn = Synapse()
        syn.login()
        # upload samplesheet to S3
        stage_samplesheet(syn, dataset)
    
    if 'all' in step or 'synstage' in step: 
        print('starting synstage')
        # stage fastq and updated samplesheetj
        synstage_info = prepare_synstage_info(dataset)
        synstage_run_id = ops.launch_workflow(synstage_info, "spot")
        status = await ops.monitor_workflow(run_id=synstage_run_id, wait_time=60 * 2)
        print(status)
        
    if 'all' in step or 'process' in step: 
        print('starting data processing pipeline')
        # run sarek pipeline
        sarek_info = prepare_sarek_launch_info(dataset)
        sarek_run_id = ops.launch_workflow(sarek_info, "spot")
        status = await ops.monitor_workflow(run_id=sarek_run_id, wait_time=60 * 2)
        print(status)
        

    if 'all' in step or 'synindex' in step: 
        print('starting synindex')
        # index the output files in Synapse
        synindex_info = prepare_synindex_launch_info(dataset)
        synindex_run_id = ops.launch_workflow(synindex_info, "spot")
        status = await ops.monitor_workflow(run_id=synindex_run_id, wait_time=60 * 2)
        print(status)

if __name__ == "__main__":
    asyncio.run(main())

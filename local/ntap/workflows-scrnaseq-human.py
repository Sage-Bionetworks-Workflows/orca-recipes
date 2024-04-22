"""https://sagebionetworks.jira.com/browse/WORKFLOWS-390
This workflow was created to run the `nf-core/scrnaseq` pipeline for GRCh38 RNASeq data.

To note: In workflows-390 it was found the initial dataset was rnaseq, not single-cell. This
workflow should still work for single-cell data.
"""
import os
import re
import asyncio
import boto3
from dataclasses import dataclass
from pathlib import Path

from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo


# Because of the cellranger pipeline, we need to modify the filenames of the staged files in S3.
# This occurs after synstage has been ran, but before the nf-core/scrnaseq pipeline is ran.
session = boto3.Session(profile_name="nextflow-prod")
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
    samplesheet: str
    samplesheet_location_prefix: str
    staging_location: str
    synapse_id_for_output: str
    run_number: int
    output_location: str

    bucket_name: str

    @property
    def samplesheet_location(self) -> str:
        return f"{self.samplesheet_location_prefix}/{self.samplesheet}"

    @property
    def staged_samplesheet_location(self) -> str:
        return f"{self.staging_location}synstage_{self.id}/{self.samplesheet}"

    @property
    def staged_samplesheet_key(self) -> str:
        location_prefix = self.staging_location.replace(f"s3://{self.bucket_name}/", "")
        return f"{location_prefix}synstage_{self.id}/{self.samplesheet}"

    @property
    def staging_location_key(self) -> str:
        location_prefix = self.staging_location.replace(f"s3://{self.bucket_name}/", "")
        return location_prefix

    @property
    def synstage_run_name(self) -> str:
        return f"synstage_{self.id}"

    @property
    def scrnaseq_run_name(self) -> str:
        return f"scrnaseq_GRCh38_{self.id}_{self.run_number}"

    @property
    def synindex_run_name(self) -> str:
        return f"synindex_{self.id}_{self.run_number}"


def generate_datasets() -> list[Dataset]:
    """Generate list of datasets.

    Source: https://sagebionetworks.jira.com/browse/WORKFLOWS-538

    This is expecting that the SampleSheet to be run exists in the `prefix` location.
    """
    return [
        # Dataset(
        #     id="syn52120378", # This is not Single-Cell RNASeq data as originally thought.
        #     samplesheet_location_prefix="s3://ntap-add5-project-tower-bucket/samplesheets/WORKFLOWS-390/to_stage/",
        #     samplesheet=f"syn51199007_RNA_Seq_SampleSheet.csv",
        #     staging_location="s3://ntap-add5-project-tower-bucket/samplesheets/Reprocess/WORKFLOWS-390/",
        #     synapse_id_for_output="syn52913106",  # TODO - SYNINDEX was not yet implemented
        #     run_number=3,
        #     output_location="s3://ntap-add5-project-tower-bucket/outputs/rnaseq_GRCh38_syn52120378/",
        #     bucket_name="ntap-add5-project-tower-bucket",
        # )
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
        nextflow_config="wave.enabled=false",
    )


def modify_filenames_for_cellranger(dataset: Dataset):
    """Handles modifying filenames on the staged files and the staged CSV file. Cellranger
    is expecting the filename pattern to match:
    https://github.com/nf-core/modules/blob/master/modules/nf-core/cellranger/count/templates/cellranger_count.py#L37
    filename_pattern =  r'([^a-zA-Z0-9])R1([^a-zA-Z0-9])'

    If the filename is not in this format, the pipeline will fail.

    We had some file names in this format:
    `SRR19761350_1.fastq.gz` and `SRR19761350_2.fastq.gz`

    This function will download the staged CSV file, modify the filenames in the CSV file,
    then upload the CSV.

    It will also modify the filenames in the staged files by doing an S3 copy of the file
    and remove of the old file.

    :param dataset: The dataset to modify the filenames for.
    """
    # Download the staged CSV file
    download_location = f"/tmp/{dataset.samplesheet}"
    s3.download_file(
        dataset.bucket_name,
        dataset.staged_samplesheet_key,
        download_location,
    )
    print(download_location)

    # Read the file in modify the filenames if they match `_1.` or `_2.` to be `_R1.` or `_R2.`
    with open(download_location, "r+") as file:
        contents = file.read()
        contents = contents.replace("_1.", "_R1.").replace("_2.", "_R2.")
        file.seek(0)
        file.write(contents)
        file.truncate()
        del contents

    s3.upload_file(
        download_location, dataset.bucket_name, dataset.staged_samplesheet_key
    )
    os.remove(download_location)

    # For each folder in the S3 `staging_location` if the directory is in the format of `syn$numbers$` then
    # rename the file in that directory if it contains _1. or _2. to be _R1. or _R2.
    objects = s3.list_objects_v2(
        Bucket=dataset.bucket_name, Prefix=dataset.staging_location_key
    )
    for obj in objects["Contents"]:
        key = obj["Key"]
        # Example key: 'samplesheets/Reprocess/WORKFLOWS-390/syn52073763/'
        if re.match(rf"{dataset.staging_location_key}syn\d+/$", key):
            files = s3.list_objects_v2(Bucket=dataset.bucket_name, Prefix=key)
            for file in files["Contents"]:
                file_key = file["Key"]
                # Example key: 'samplesheets/Reprocess/WORKFLOWS-390/syn52073763/SRR19761350_1.fastq.gz'
                if re.match(
                    rf"{dataset.staging_location_key}syn\d+/.*_[1,2]\.fastq\.gz$",
                    file_key,
                ):
                    # Replace _1. with _R1. and _2. with _R2.
                    new_key = re.sub(r"_1\.", "_R1.", file_key)
                    new_key = re.sub(r"_2\.", "_R2.", new_key)

                    print(f"Moving {file_key} to {new_key}")

                    # Copy the object to the new key
                    s3.copy(
                        {"Bucket": dataset.bucket_name, "Key": file_key},
                        dataset.bucket_name,
                        new_key,
                    )

                    print(f"Deleting {file_key}")
                    # Delete the object with the old key
                    s3.delete_object(Bucket=dataset.bucket_name, Key=file_key)


def prepare_scrnaseq_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/scrnaseq workflow run."""
    return LaunchInfo(
        run_name=dataset.scrnaseq_run_name,
        pipeline="nf-core/scrnaseq",
        revision="2.4.1",
        profiles=["sage"],
        params={
            "input": dataset.staged_samplesheet_location,
            "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{dataset.scrnaseq_run_name}/",
            "aligner": "cellranger",
            "genome_fasta": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Sequence/WholeGenomeFasta/genome.fa",
            "fasta": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Sequence/WholeGenomeFasta/genome.fa",
            "gtf": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Annotation/Genes/genes.gtf",
            "star_index": "s3://sage-igenomes/igenomes/Homo_sapiens/NCBI/GRCh38/Sequence/STARIndex/",
        },
        pre_run_script="NXF_VER=23.10.0",
        nextflow_config=Path("local/ntap/nextflow-scrnaseq.config").read_text(),
    )


# def prepare_synindex_launch_info(dataset: Dataset) -> LaunchInfo:
#     """Generate LaunchInfo for nf-synindex workflow run."""
#     return LaunchInfo(
#         run_name=dataset.synindex_run_name,
#         pipeline="Sage-Bionetworks-Workflows/nf-synindex",
#         revision="disable_wave",
#         profiles=["sage"],
#         params={
#             "s3_prefix": dataset.output_location,
#             "parent_id": dataset.synapse_id_for_output,
#         },
#         pre_run_script="NXF_VER=22.10.4",
#         workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
#         nextflow_config="wave.enabled=false",
#     )


async def run_workflows(ops: NextflowTowerOps, dataset: Dataset):
    synstage_info = prepare_synstage_info(dataset)
    synstage_run_id = ops.launch_workflow(synstage_info, "spot")
    status = await ops.monitor_workflow(run_id=synstage_run_id, wait_time=60 * 2)
    print(status)

    modify_filenames_for_cellranger(dataset)

    scrnaseq_info = prepare_scrnaseq_launch_info(dataset)
    scrnaseq_run_id = ops.launch_workflow(scrnaseq_info, "spot")
    status = await ops.monitor_workflow(run_id=scrnaseq_run_id, wait_time=60 * 2)
    print(status)

    # synindex_info = prepare_synindex_launch_info(dataset)
    # synindex_run_id = ops.launch_workflow(synindex_info, "spot")
    # status = await ops.monitor_workflow(run_id=synindex_run_id, wait_time=60 * 2)
    # print(status)


if __name__ == "__main__":
    asyncio.run(main())

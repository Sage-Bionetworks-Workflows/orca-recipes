import argparse
from multiprocessing import Pool
import os
import subprocess
from typing import Dict

import pandas as pd
import numpy as np
import synapseclient
import synapseutils

import utils

my_agent = "iatlas-cbioportal/0.0.0"
syn = synapseclient.Synapse(user_agent=my_agent).login()


def read_and_merge_maf_files(input_folder_synid: str) -> pd.DataFrame:
    """Read in and merge MAF files from a specified folder
    
    Args:
        folder: Synapse id of folder containing MAF files
        
    Return:
        pd.DataFrame: Merged maf of all mafs in input folder
    """
    entities = syn.getChildren(input_folder_synid)
    # Filter for files ending in .maf
    dfs = []
    for item in entities:
        if item['name'].endswith('.maf'):
            df = pd.read_csv(syn.get(item['id']).path, sep="\t", comment="#")
            dfs.append(df)

    if not dfs:
        print(f"No MAF files found in  {input_folder_synid:}")
        return None
    else:
        merged_df = pd.concat(dfs, ignore_index=True)
        return merged_df


def split_into_chunks(
    dataset_name: str,
    input_df: pd.DataFrame,
    datahub_tools_path: str,
    max_rows: int = 40000,
) -> int:
    """Splits the maf file into designated number of equal (if possible) chunks.
       Maf files don't need to be randomnly shuffled prior because the
       variants are independent of one another.

       Genome nexus cannot handle maf file sizes greater than ~55 MB

    Args:
        dataset_name (str): Name of the dataset
        input_df (pd.DataFrame): Input maf data to be split
        datahub_tools_path (str): Path to the datahub tools repo
        max_rows (int): maximum rows to split each chunk into

    Returns:
        int: Number of maf chunks
    """
    dataset_dir = os.path.join(
        f"{datahub_tools_path}/add-clinical-header/", dataset_name
    )
    n_maf_chunks = (len(input_df) + max_rows - 1) // max_rows  # ceiling division
    for i in range(n_maf_chunks):
        maf_chunk = input_df[i * max_rows : (i + 1) * max_rows]
        maf_chunk.to_csv(
            f"{dataset_dir}/data_mutations_{i + 1}.txt",
            sep="\t",
            index=False,
            float_format="%.12g",
        )
    return n_maf_chunks


def run_genome_nexus(
    dataset_name: str, n_maf_chunks: int, n_workers: int, datahub_tools_path: str
) -> None:
    """Runs genome nexus annotator on each of the split maf chunks. Logging
        is saved for each chunk.

    Args:
        dataset_name (str): Name of the dataset
        n_maf_chunks (int): Number of chunks the maf was split into
        n_workers (int): Number of worker processes to use to
            run the genome nexus annotator in parallel
        datahub_tools_path (str): Path to the datahub tools repo
    """
    dataset_dir = os.path.join(
        f"{datahub_tools_path}/add-clinical-header/", dataset_name
    )

    args_list = [(i, dataset_dir) for i in range(1, n_maf_chunks + 1)]

    with Pool(processes=n_workers) as pool:
        pool.starmap(run_genome_nexus_for_one_chunk, args_list)


def run_genome_nexus_for_one_chunk(i: int, dataset_dir: str) -> None:
    """Worker script for the run_genome_nexus main script. Runs
        genome nexus on one maf chunk

    Args:
        i (int): maf chunk number
        dataset_dir (str): the directory to pull the maf chunks from
    """
    filename = f"data_mutations_{i}.txt"
    cmd = f"""
    docker run --rm -it -e GENOMENEXUS_BASE=https://grch38.genomenexus.org \
        -v {dataset_dir}:/data genomenexus/gn-annotation-pipeline:master java \
        -jar annotationPipeline.jar \
        --filename /data/{filename} \
        --output-filename /data/data_mutations_annotated_{i}.txt \
        --error-report-location /data/data_mutations_error_report_{i}.txt \
        --isoform-override mskcc
    """
    with open(f"{dataset_dir}/genome_nexus_log_chunk_{i}.txt", "w") as f:
        subprocess.run(
            cmd,
            shell=True,
            executable="/bin/bash",
            check=True,
            stdout=f,
            stderr=subprocess.STDOUT,
        )


def concatenate_mafs(
    dataset_name: str, n_maf_chunks: int, datahub_tools_path: str
) -> Dict[str, pd.DataFrame]:
    """Concatenates the annotated mafs together
        and the error reports of the failed annotations together

    Args:
        dataset_name (str): Name of dataset
        n_maf_chunks (int): Number of maf chunks
        datahub_tools_path (str): Path to the datahub tools repo

    Returns:
        Dict[str, pd.DataFrame]: A dictionary with the following keys:
            "annotated_maf" : the annotated maf dataset concatenated together
            "error_maf" : error report outputted by genome nexus of failed annotations
    """
    dataset_dir = os.path.join(
        f"{datahub_tools_path}/add-clinical-header/", dataset_name
    )
    annotated_mafs = []
    error_mafs = []
    for i in range(1, n_maf_chunks + 1):
        annotated_mafs.append(
            pd.read_csv(
                f"{dataset_dir}/data_mutations_annotated_{i}.txt", sep="\t", comment="#"
            )
        )
        error_mafs.append(
            pd.read_csv(f"{dataset_dir}/data_mutations_error_report_{i}.txt", sep="\t")
        )

    annotated_mafs_all = pd.concat(annotated_mafs)
    error_mafs_all = pd.concat(error_mafs)

    annotated_mafs_all_processed = postprocessing(input_df=annotated_mafs_all)
    annotated_mafs_all_processed.to_csv(
        f"{dataset_dir}/data_mutations.txt", sep="\t", index=False, float_format="%.12g"
    )

    error_mafs_all.to_csv(
        f"{dataset_dir}/data_mutations_error_report.txt",
        sep="\t",
        index=False,
        float_format="%.12g",
    )
    return {"annotated_maf": annotated_mafs_all, "error_maf": error_mafs_all}


def postprocessing(input_df: pd.DataFrame) -> pd.DataFrame:
    """Additional postprocessing steps we have to do for the
       datasets. This can be removing variants that
       cannot be annotated, etc.

    Args:
        input_df (pd.DataFrame): input annotated maf file

    Returns:
        pd.DataFrame: output maf file postprocessed
    """
    annotated_maf = input_df.copy()
    # remove the failed variants on MT chromosome
    annotated_maf = annotated_maf[annotated_maf.Chromosome != "chrM"]
    return annotated_maf


def save_to_synapse(
    dataset_name: str,
    datahub_tools_path: str,
    output_folder_synid: str,
    version_comment: str = None,
) -> None:
    """Saves the dataset's annotated maf file and error report file
        to synapse

    Args:
        dataset_name (str): name of the iatlas dataset to save to
            synapse
        datahub_tools_path (str): Path to the datahub tools repo
        output_folder_synid (str): Synapse id of the output folder
        version_comment (str): Version comment for this iteration of files on synapse. Optional.
            Defaults to None.
    """
    dataset_dir = os.path.join(datahub_tools_path, "add-clinical-header", dataset_name)
    # see if dataset_folder exists
    dataset_folder_exists = False
    for _, directory_names, _ in synapseutils.walk(syn=syn, synId=output_folder_synid):
        directories = directory_names  # top level directories
        break

    for dataset_folder in directories:
        if dataset_name == dataset_folder[0]:
            dataset_folder_exists = True
            dataset_folder_id = dataset_folder[1]
            break

    if not dataset_folder_exists:
        new_dataset_folder = synapseclient.Folder(
            dataset_name, parent=output_folder_synid
        )
        dataset_folder_id = syn.store(new_dataset_folder).id

    syn.store(
        synapseclient.File(
            f"{dataset_dir}/data_mutations.txt",
            parent=dataset_folder_id,
            version_comment=version_comment,
        )
    )
    syn.store(
        synapseclient.File(
            f"{dataset_dir}/data_mutations_error_report.txt",
            parent=dataset_folder_id,
            version_comment=version_comment,
        )
    )


def generate_meta_files(dataset_name: str, datahub_tools_path: str) -> None:
    """Generates the meta* files for the given dataset

    Args:
        dataset_name (str): name of the iatlas dataset
        datahub_tools_path (str): Path to the datahub tools repo
    """
    dataset_dir = os.path.join(
        f"{datahub_tools_path}/add-clinical-header/", dataset_name
    )

    cmd = f"""
    python3 {datahub_tools_path}/generate-meta-files/generate_meta_files.py \
        -d {dataset_dir} \
        -s iatlas_{dataset_name} \
        -m {datahub_tools_path}/generate-meta-files/datatypes.txt
    """
    # Run in shell to allow sourcing
    subprocess.run(cmd, shell=True, executable="/bin/bash")


def validate_export_files(input_df: pd.DataFrame, output_df: pd.DataFrame) -> None:
    """Validates the export files, checking rows

    Args:
        input_df (pd.DataFrame): input maf data
        output_df (pd.DataFrame): output annotated maf data
    """
    start_rows = input_df.shape[0]
    assert start_rows == len(output_df)
    # no dups
    assert len(output_df[output_df.duplicated()]) == 0
    # check that the SAMPLE_ID/Tumor_Sample_Barcode exists in original maf
    assert set(list(output_df.Tumor_Sample_Barcode.unique())) == set(
        list(input_df.Tumor_Sample_Barcode.unique())
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=str,
        help="Name of dataset to run processing for",
    )
    parser.add_argument(
        "--input_folder_synid",
        type=str,
        help="Synapse id for the input folder containing all of the mafs to be merged",
    )
    parser.add_argument(
        "--max_rows",
        type=int,
        help="Max rows per maf chunk used to split maf file. Default: 40000",
        default=40000,
    )
    parser.add_argument(
        "--output_folder_synid",
        type=str,
        help="Synapse id for output folder to store the export files",
    )
    parser.add_argument(
        "--datahub_tools_path",
        type=str,
        help="Path to datahub-study-curation-tools repo",
    )
    parser.add_argument(
        "--n_workers",
        type=int,
        default=3,
        help="Number of worker processes to run genome nexus in parallel. Optional. Defaults to 3.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        default=False,
        help="Whether to run without saving to Synapse",
    )
    parser.add_argument(
        "--clear_workspace",
        action="store_true",
        default=False,
        help="Whether to clear local directory of files or not",
    )
    parser.add_argument(
        "--version_comment",
        default=None,
        type=str,
        help="Version comment for the files on Synapse. Optional. Defaults to None.",
    )

    args = parser.parse_args()
    if args.clear_workspace:
        utils.clear_workspace(dir_path=f"{args.datahub_tools_path}/add-clinical-header")

    maf_df = read_and_merge_maf_files(input_folder_synid = args.input_folder_synid)
    n_maf_chunks = split_into_chunks(
        dataset_name=args.dataset,
        input_df=maf_df,
        datahub_tools_path=args.datahub_tools_path,
        max_rows=args.max_rows,
    )
    run_genome_nexus(
        dataset_name=args.dataset,
        n_maf_chunks=n_maf_chunks,
        n_workers=args.n_workers,
        datahub_tools_path=args.datahub_tools_path,
    )
    mafs = concatenate_mafs(
        dataset_name=args.dataset,
        n_maf_chunks=n_maf_chunks,
        datahub_tools_path=args.datahub_tools_path,
    )
    validate_export_files(input_df=maf_df, output_df=mafs["annotated_maf"])
    generate_meta_files(
        dataset_name=args.dataset, datahub_tools_path=args.datahub_tools_path
    )
    if not args.dry_run:
        save_to_synapse(
            dataset_name=args.dataset,
            datahub_tools_path=args.datahub_tools_path,
            output_folder_synid=args.output_folder_synid,
            version_comment=args.version_comment,
        )


if __name__ == "__main__":
    main()

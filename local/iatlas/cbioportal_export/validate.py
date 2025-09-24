import argparse
import logging
import os
import subprocess
from typing import Dict

import pandas as pd

import utils

syn = utils.synapse_login()


def validate_that_neoantigen_maf_ids_are_equal(
    input_df: pd.DataFrame, neoantigen_data_synid: pd.DataFrame, **kwargs
) -> None:
    """Checks that the Tumor_Sample_Barcode values in the maf data match the
        Sample_ID values in the neoantigen summary counts data

    Args:
        maf_df (pd.DataFrame): Maf processed input data
        neoantigen_data_synid (pd.DataFrame): Neoantigen data (prior to merge with sample clinical data)
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    neoantigen_data = pd.read_csv(syn.get(neoantigen_data_synid).path, sep="\t")
    neoantigen_data["Sample_ID"] = neoantigen_data["Sample_ID"].astype(str)
    if set(input_df["Tumor_Sample_Barcode"].unique()) != set(
        neoantigen_data["Sample_ID"].unique()
    ):
        logger.error(
            "The Tumor_Sample_Barcode values in the maf data do not match the Sample_ID values in the neoantigen data."
        )


def get_all_files_to_validate(
    dataset_name: str, datahub_tools_path: str
) -> Dict[str, pd.DataFrame]:
    """This pulls in all of the datasets needed for validation

    Args:
        dataset_name (str): name of the dataset to validate
        datahub_tools_path (str): local path to the datahub-tools repo

    Returns:
        Dict[str, pd.DataFrame]: dictionary where each key-value pair is
            file_name : file read in as pandas dataframe
    """
    dataset_dir = utils.get_local_dataset_output_folder_path(
        dataset_name, datahub_tools_path
    )
    all_files = {}
    for file in utils.REQUIRED_OUTPUT_FILES:
        all_files[file] = pd.read_csv(os.path.join(dataset_dir, file), sep="\t")
    return all_files


def run_cbioportal_validator(
    dataset_name: str, cbioportal_path: str, datahub_tools_path: str, **kwargs
) -> None:
    """Runs the cbioportal validation script to check the
        input clinical, metadata files and saves the output

    Args:
        dataset_name (str): name of the dataset
        cbioportal_path (str): Path to cbioportal repo containing validator script
        datahub_tools_path (str): path to the datahub tools repo containing
            the locally saved clinical files
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    cmd = f"""
    python3 {cbioportal_path}/core/src/main/scripts/importer/validateData.py \
        -s "{datahub_tools_path}/add-clinical-header/{dataset_name}" \
            --no_portal_checks \
            --strict_maf_checks
    """
    validated = f"{datahub_tools_path}/add-clinical-header/{dataset_name}/cbioportal_validator_output.txt"
    with open(f"{validated}", "w") as outfile:
        subprocess.run(
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=outfile,
            stderr=subprocess.STDOUT,
        )
    logger.info(f"cbioportal validator results saved to: {validated}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=str,
        help="Name of dataset to run processing for",
    )
    parser.add_argument(
        "--datahub_tools_path",
        type=str,
        help="Path to datahub-study-curation-tools repo",
    )
    parser.add_argument(
        "--neoantigen_data_synid",
        type=str,
        help="Synapse id for the summary neoantigen data that needs to be merged into the clinical.",
    )
    parser.add_argument(
        "--cbioportal_path",
        type=str,
        help="Path to cbioportal repo",
    )
    args = parser.parse_args()

    all_files = get_all_files_to_validate(
        dataset_name=args.dataset, datahub_tools_path=args.datahub_tools_path
    )
    dataset_flagger = utils.ErrorFlagHandler()
    dataset_logger = utils.create_logger(
        dataset_name=args.dataset,
        datahub_tools_path=args.datahub_tools_path,
        log_file_name="iatlas_validation_log.txt",
        flagger=dataset_flagger
    )
    validate_that_neoantigen_maf_ids_are_equal(
        input_df=all_files["data_mutations.txt"],
        neoantigen_data_synid=args.neoantigen_data_synid,
        logger=dataset_logger,
    )
    run_cbioportal_validator(
        dataset_name=args.dataset,
        cbioportal_path=args.cbioportal_path,
        datahub_tools_path=args.datahub_tools_path,
        logger=dataset_logger,
    )
    if dataset_flagger.had_error:
        dataset_logger.error("FAILED: Validation of study failed")
    
if __name__ == "__main__":
    main()

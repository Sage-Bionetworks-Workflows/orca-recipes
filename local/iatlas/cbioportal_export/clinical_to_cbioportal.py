import argparse
from collections import defaultdict
import csv
import os
import subprocess
import sys
from typing import Dict

import numpy as np
import pandas as pd
import synapseclient
import synapseutils


my_agent = "iatlas-cbioportal/0.0.0"
syn = synapseclient.Synapse(user_agent=my_agent).login()

EXTRA_COLS = ["Dataset"]

IATLAS_DATASETS = [
    "AMADEUS",
    "Chen_CanDisc_2016",
    "Choueiri_CCR_2016",
    "Gide_Cell_2019",
    "HTAN_OHSU",
    "HugoLo_IPRES_2016",
    "IMmotion150",
    "IMVigor210",
    "Kim_NatMed_2018",
    "Liu_NatMed_2019",
    "Melero_GBM_2019",
    "Miao_Science_2018",
    "PORTER",
    "Prat_CanRes_2017",
    "PRINCE",
    "Prins_GBM_2019",
    "Riaz_Nivolumab_2017",
    "VanAllen_antiCTLA4_2015",
    "Zhao_NatMed_2019",
]

CBIOPORTAL_METADATA_COLS = [
    "NORMALIZED_COLUMN_HEADER",
    "ATTRIBUTE_TYPE",
    "DATATYPE",
    "DESCRIPTIONS",
    "DISPLAY_NAME",
    "PRIORITY",
]

CASE_LIST_TEXT_TEMPLATE = (
    "cancer_study_identifier: {study_id}\n"
    "stable_id: {stable_id}\n"
    "case_list_name: {case_list_name}\n"
    "case_list_description: {case_list_description}\n"
    "case_list_ids: {case_list_ids}"
)


def preprocessing(
    input_df_synid: str,
    features_df_synid: str,
    cli_to_cbio_mapping: pd.DataFrame,
    cli_to_oncotree_mapping_synid: str,
    datahub_tools_path: str,
) -> pd.DataFrame:
    """Preprocesses the data, runs the individual steps:
        1. Gets the input clinical data
        2. Gets the feature data and merged it in
        3. Merges in the oncotree mappings
        4. Remaps the columns to be cbioportal headers
        5. Converts the oncotree codes to have the CANCER_TYPE and CANCER_TYPE_DETAILED columns
        6. Updates the clinical_attributes_metadata.txt in prep for adding clinical headers

    Args:
        input_df_synid (str): Synapse id of input iatlas clinical dataset
        features_df_synid (str): Synapse id of the features dataset
        cli_to_cbio_mapping (pd.DataFrame): Clinical to cbioportal attirbutes mapping
        cli_to_oncotree_mapping_synid (str): Oncotree mapping for clinical dataset
        datahub_tools_path (str): Path to the datahub tools repo

    Returns:
        pd.DataFrame: preprocessed clinical merged dataset
    """
    input_df = pd.read_csv(syn.get(input_df_synid).path, sep="\t")
    features_df = pd.read_csv(
        syn.get(features_df_synid).path, sep="\t", low_memory=True
    )
    input_df = input_df.merge(features_df, how="left", on="sample_name")

    cli_to_oncotree_mapping = pd.read_csv(
        syn.get(cli_to_oncotree_mapping_synid).path, sep="\t"
    )
    oncotree_merge_cols = ["Cancer_Tissue", "TCGA_Study", "AMADEUS_Study"]

    cli_with_oncotree = input_df.merge(
        cli_to_oncotree_mapping[oncotree_merge_cols + ["ONCOTREE_CODE"]],
        how="left",
        on=oncotree_merge_cols,
    )
    cli_to_cbio_mapping_dict = dict(
        zip(
            cli_to_cbio_mapping["iATLAS_attribute"],
            cli_to_cbio_mapping["NORMALIZED_HEADER"],
        )
    )
    cli_remapped = cli_with_oncotree.rename(columns=cli_to_cbio_mapping_dict)
    cli_remapped.rename(
        columns={"sample_name": "SAMPLE_ID", "patient_name": "PATIENT_ID"}, inplace=True
    )

    cli_remapped.to_csv(
        f"{datahub_tools_path}/add-clinical-header/cli_remapped.csv",
        index=False,
        sep="\t",
    )
    cli_w_cancer_types = convert_oncotree_codes(datahub_tools_path)
    get_updated_cli_attributes(cli_to_cbio_mapping, datahub_tools_path)

    return cli_w_cancer_types
    
    
def split_into_patient_and_sample_data(
    input_data: pd.DataFrame, 
    cli_to_cbio_mapping : pd.DataFrame
    )-> Dict[str, pd.DataFrame]:
    """ This splits the preprocessed clinical dataset (prior to adding clinical headers)
    into patient and sample datasets
    
    Args:
        input_df (pd.DataFrame): Input iatlas merged preprocessed clinical dataset
        cli_to_cbio_mapping (pd.DataFrame): Clinical to cbioportal attirbutes mapping

    Returns:
        Dict[str, pd.DataFrame]: A dictionary with the following keys:
            "merged" : the clinical merged file before split
            "patient" : patient dataset
            "sample" : sample dataset
    """
    patient_cols = ["PATIENT_ID"] + list(
        cli_to_cbio_mapping[
            cli_to_cbio_mapping.ATTRIBUTE_TYPE == "PATIENT"
        ].NORMALIZED_HEADER.unique()
    )
    sample_cols = [
        "SAMPLE_ID",
        "PATIENT_ID",
        "CANCER_TYPE",
        "CANCER_TYPE_DETAILED",
    ] + list(
        cli_to_cbio_mapping[
            cli_to_cbio_mapping.ATTRIBUTE_TYPE == "SAMPLE"
        ].NORMALIZED_HEADER.unique()
    )
    return {
        "merged": input_data,
        "patient": input_data[patient_cols + EXTRA_COLS].drop_duplicates(),
        "sample": input_data[sample_cols + EXTRA_COLS],
    }


def get_cli_to_cbio_mapping(cli_to_cbio_mapping_synid: str) -> pd.DataFrame:
    """Gets the mapping of the iatlas clinical attributes to cbioportal
        attributes

    Args:
        cli_to_cbio_mapping_synid (str): synapse id of the clinical
        attributes to cbioportal attributes

    Returns:
        pd.DataFrame: mapping
    """
    cli_to_cbio_mapping = pd.read_csv(syn.get(cli_to_cbio_mapping_synid).path, sep="\t")
    return cli_to_cbio_mapping


def convert_floats_in_priority_column(input_df : pd.DataFrame) -> pd.DataFrame:
    """Converts the floating point 1.0 in PRIORITY column to 1s.
        This is due to pandas behavior of converting integers to floats
        in a mixed dtype column with NAs

    Args:
        input_df (pd.DataFrame): input data with PRIORITY column

    Returns:
        pd.DataFrame: input_df with PRIORITY column converted to "1" instead of 1.0
    """
    converted_df = input_df.copy()
    # Coerce PRIORITY to numeric, setting errors='coerce' turns non-numeric values into NaN
    converted_df['PRIORITY_NUM'] = pd.to_numeric(converted_df['PRIORITY'], errors='coerce')

    # Now apply isclose safely
    converted_df.loc[np.isclose(converted_df['PRIORITY_NUM'], 1.0), 'PRIORITY'] = '1'
    converted_df.drop(columns='PRIORITY_NUM', inplace=True)
    return(converted_df)
    

def get_updated_cli_attributes(
    cli_to_cbio_mapping: pd.DataFrame, datahub_tools_path: str
):
    """Updates the cbioportal clinical_attributes_metadata attributes used in
        creating the cbioportal clinical headers

    Args:
        cli_to_cbio_mapping_synid (str): synapse id of the clinical
            to cbioportal attributes mapping
        datahub_tools_path (str): Path to the datahub tools repo
    """
    cli_attr = pd.read_csv(
        f"{datahub_tools_path}/add-clinical-header/clinical_attributes_metadata.txt",
        sep="\t",
    )
    cli_to_cbio_mapping_to_append = cli_to_cbio_mapping.rename(
        columns={
            "NORMALIZED_HEADER": "NORMALIZED_COLUMN_HEADER",
            "DESCRIPTION": "DESCRIPTIONS",
            "DATA_TYPE": "DATATYPE",
        }
    )
    cli_to_cbio_mapping_to_append = cli_to_cbio_mapping_to_append[
        CBIOPORTAL_METADATA_COLS
    ]
    cli_attr_full = pd.concat([cli_attr, cli_to_cbio_mapping_to_append])
    cli_attr_full = cli_attr_full.drop_duplicates(
        subset="NORMALIZED_COLUMN_HEADER", keep="last"
    )
    # resolve pandas int to float conversion issue
    cli_attr_full = convert_floats_in_priority_column(input_df = cli_attr_full)
    cli_attr_full.to_csv(
        f"{datahub_tools_path}/add-clinical-header/clinical_attributes_metadata.txt",
        sep="\t",
    )
    return cli_attr_full


def convert_oncotree_codes(datahub_tools_path: str) -> pd.DataFrame:
    """Converts the oncotree codes to CANCER_TYPE and CANCER_TYPE_DESCRIPTION

    Args:
        datahub_tools_path (str): Path to the datahub tools repo

    Returns:
        pd.DataFrame: returns the converted clinical data with the new columns
    """

    cmd = f"""
        python3 {datahub_tools_path}/oncotree-code-converter/oncotree_code_converter.py \
            --clinical-file {datahub_tools_path}/add-clinical-header/cli_remapped.csv
    """
    # Run in shell to allow sourcing
    subprocess.run(cmd, shell=True, executable="/bin/bash")
    cli_w_cancer_types = pd.read_csv(
        f"{datahub_tools_path}/add-clinical-header/cli_remapped.csv", sep="\t"
    )
    return cli_w_cancer_types


def add_clinical_header(
    input_dfs: pd.DataFrame,
    dataset_name: str,
    datahub_tools_path: str,
) -> None:
    """Adds the clinical headers to the patient and sample data
        by calling cbioportal repo
        
    Args:
        cli_df (pd.DataFrame): input clinical dataframe with all mappings
        dataset_name (str): name of dataset to add clinical headers to
        datahub_tools_path (str): Path to the datahub tools repo
    """

    dataset_dir = os.path.join(
        f"{datahub_tools_path}/add-clinical-header/", dataset_name
    )
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir)

    patient_df_subset = input_dfs["patient"][
        input_dfs["patient"]["Dataset"] == dataset_name
    ]
    patient_df_subset.drop(columns=list(EXTRA_COLS)).to_csv(
        f"{dataset_dir}/data_clinical_patient.txt", sep="\t", index=False
    )
    sample_df_subset = input_dfs["sample"][
        input_dfs["sample"]["Dataset"] == dataset_name
    ]
    sample_df_subset.drop(columns=list(EXTRA_COLS)).to_csv(
        f"{dataset_dir}/data_clinical_sample.txt", sep="\t", index=False
    )
    cmd = f"""
    cd {datahub_tools_path}/add-clinical-header/
    python3 {datahub_tools_path}/add-clinical-header/insert_clinical_metadata.py \
        -d {dataset_dir}    
    """
    # Run in shell to allow sourcing
    subprocess.run(cmd, shell=True, executable="/bin/bash")

    # saved merged for case lists
    merged_df_subset = input_dfs["merged"][
        input_dfs["merged"]["Dataset"] == dataset_name
    ]
    merged_df_subset.drop(columns=list(EXTRA_COLS)).to_csv(
        f"{dataset_dir}/data_clinical_merged.txt", sep="\t", index=False
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


def create_case_lists_map(clinical_file_name: str):
    """
    Creates the case list dictionary

    Args:
        clinical_file_name: clinical file path

    Returns:
        dict: key = cancer_type
              value = list of sample ids
        dict: key = seq_assay_id
              value = list of sample ids
        list: Clinical samples
    """
    with open(clinical_file_name, "r", newline=None) as clinical_file:
        clinical_file_map = defaultdict(list)
        clin_samples = []
        reader = csv.DictReader(clinical_file, dialect="excel-tab")
        for row in reader:
            clinical_file_map[row["CANCER_TYPE"]].append(row["SAMPLE_ID"])
            clin_samples.append(row["SAMPLE_ID"])
    return clinical_file_map, clin_samples


def write_single_oncotree_case_list(
    cancer_type: str, ids: list, study_id: str, output_directory: str
) -> str:
    """
    Writes one oncotree case list. Python verisons below
    3.6 will sort the dictionary keys which causes tests to fail

    Args:
        cancer_type: Oncotree code cancer type
        ids: GENIE sample ids
        study_id: cBioPortal study id
        output_directory: case list output directory

    Returns:
        case list file path
    """
    cancer_type = "NA" if cancer_type == "" else cancer_type
    cancer_type_no_spaces = (
        cancer_type.replace(" ", "_").replace(",", "").replace("/", "_")
    )
    cancer_type_no_spaces = (
        "no_oncotree_code" if cancer_type_no_spaces == "NA" else cancer_type_no_spaces
    )
    case_list_text = CASE_LIST_TEXT_TEMPLATE.format(
        study_id=study_id,
        stable_id=study_id + "_" + cancer_type_no_spaces,
        case_list_name="Tumor Type: " + cancer_type,
        case_list_description="All tumors with cancer type " + cancer_type,
        case_list_ids="\t".join(ids),
    )
    case_list_path = os.path.abspath(
        os.path.join(output_directory, "cases_" + cancer_type_no_spaces + ".txt")
    )
    with open(case_list_path, "w") as case_list_file:
        case_list_file.write(case_list_text)
    return case_list_path


def write_case_list_all(clinical_samples: list, output_directory: str, study_id: str):
    """
    Writes the genie all samples.

    Args:
        clinical_samples: List of clinical samples
        output_directory: Directory to write case lists
        study_id: cBioPortal study id

    Returns:
        list: case list sequenced and all
    """
    caselist_files = []
    case_list_ids = "\t".join(clinical_samples)
    cases_all_path = os.path.abspath(os.path.join(output_directory, "cases_all.txt"))
    with open(cases_all_path, "w") as case_list_all_file:
        case_list_file_text = CASE_LIST_TEXT_TEMPLATE.format(
            study_id=study_id,
            stable_id=study_id + "_all",
            case_list_name="All samples",
            case_list_description="All samples",
            case_list_ids=case_list_ids,
        )
        case_list_all_file.write(case_list_file_text)
    return caselist_files


def write_case_list_files(
    clinical_file_map: dict, output_directory: str, study_id: str
):
    """
    Writes the cancer_type case list file to case_lists directory

    Args:
        clinical_file_map: cancer type to sample id mapping from
                           create_case_lists_map
        output_directory: Directory to write case lists
        study_id: cBioPortal study id

    Returns:
        list: oncotree code case list files
    """
    case_list_files = []
    for cancer_type, ids in clinical_file_map.items():
        case_list_path = write_single_oncotree_case_list(
            cancer_type, ids, study_id, output_directory
        )
        case_list_files.append(case_list_path)
    return case_list_files


def create_case_lists(clinical_file_name: str, output_directory: str, study_id: str):
    """Gets clinical file and gene matrix file and processes it
    to obtain case list files

    Args:
        clinical_file_name: Clinical file path
        assay_info_file_name: Assay information name
        output_directory: Output directory of case list files
        study_id: cBioPortal study id
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    case_lists_map, clin_samples = create_case_lists_map(clinical_file_name)
    write_case_list_files(case_lists_map, output_directory, study_id)
    write_case_list_all(clin_samples, output_directory, study_id)


def save_to_synapse(dataset_name: str, datahub_tools_path: str, output_folder_synid : str) -> None:
    """Saves the dataset's clinical file, case lists
        and meta files to its synapse respective folders

    Args:
        dataset_name (str): name of the iatlas dataset to save to
            synapse
        datahub_tools_path (str): Path to the datahub tools repo
        output_folder_synid (str): Synapse id of the output folder
    """
    # TODO: Make into argument
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
    # store clinical patient file
    syn.store(
        synapseclient.File(
            f"{dataset_dir}/data_clinical_patient.txt",
            name="data_clinical_patient.txt",
            parent=dataset_folder_id,
        )
    )
    # store clinical sample file
    syn.store(
        synapseclient.File(
            f"{dataset_dir}/data_clinical_sample.txt",
            name="data_clinical_sample.txt",
            parent=dataset_folder_id,
        )
    )
    # store meta* files
    syn.store(
        synapseclient.File(
            f"{dataset_dir}/meta_clinical_patient.txt", parent=dataset_folder_id
        )
    )
    syn.store(
        synapseclient.File(
            f"{dataset_dir}/meta_clinical_sample.txt", parent=dataset_folder_id
        )
    )
    syn.store(
        synapseclient.File(f"{dataset_dir}/meta_study.txt", parent=dataset_folder_id)
    )
    case_list_files = os.listdir(os.path.join(dataset_dir, "case_lists"))
    case_list_folder = synapseclient.Folder("case_lists", parent=dataset_folder_id)
    try:
        case_list_folder_id = syn.store(case_list_folder).id
    except:
        sys.exit(-1)
    for file in case_list_files:
        syn.store(
            synapseclient.File(
                f"{dataset_dir}/case_lists/{file}", parent=case_list_folder_id
            )
        )


def validate_export_files(
    input_df_synid: str, dataset_name: str, datahub_tools_path: str
) -> None:
    """Does simple validation of the sample and patient count
        for the input and output clincial files

    Args:
        input_df_synid (str): input clinical file synapse id
        dataset_name (str): name of the iatlas dataset to validate
        datahub_tools_path (str): Path to the datahub tools repo
    """
    input_df = pd.read_csv(syn.get(input_df_synid).path, sep="\t")
    cli_df_subset = input_df[input_df["Dataset"] == dataset_name]
    dataset_dir = os.path.join(
        f"{datahub_tools_path}/add-clinical-header/", dataset_name
    )

    output_patient_df = pd.read_csv(
        os.path.join(dataset_dir, f"data_clinical_patient.txt"),
        sep="\t",
        skiprows=4,
    )

    output_samples_df = pd.read_csv(
        os.path.join(dataset_dir, f"data_clinical_sample.txt"),
        sep="\t",
        skiprows=4,
    )
    n_samples_start = len(cli_df_subset.sample_name.unique())
    n_patients_start = len(cli_df_subset.patient_name.unique())
    n_samples_end = len(output_samples_df.SAMPLE_ID.unique())
    n_patients_end = len(output_patient_df.PATIENT_ID.unique())

    if len(cli_df_subset) != len(output_samples_df):
        print(
            f"Input is {len(cli_df_subset)} rows, output is {len(output_samples_df)} rows"
        )
    if n_samples_start != n_samples_end:
        print(
            f"There are {n_samples_start} samples start, there are {n_samples_end} samples end"
        )
    if n_patients_start != n_patients_end:
        print(
            f"There are {n_patients_start} patients start, there are {n_patients_end} patients end"
        )
    print("\n\n")


def run_cbioportal_validator(
    dataset_name: str, cbioportal_path: str, datahub_tools_path: str
) -> None:
    """Runs the cbioportal validation script to check the
        input clinical, metadata files and saves the output

    Args:
        dataset_name (str): name of the dataset
        cbioportal_path (str): Path to cbioportal repo containing validator script
        datahub_tools_path (str): path to the datahub tools repo containing
            the locally saved clinical files
    """
    cmd = f"""
    python3 {cbioportal_path}/core/src/main/scripts/importer/validateData.py \
        -s "{datahub_tools_path}/add-clinical-header/{dataset_name}" -n
    """
    validated = f"{datahub_tools_path}/add-clinical-header/{dataset_name}/cbioportal_validator_output.txt"
    with open(f"{validated}", "w") as outfile:
        subprocess.run(
            cmd, 
            shell=True, 
            executable="/bin/bash",
            stdout=outfile, 
            stderr=subprocess.STDOUT
        )
    print(f"cbioportal validator results saved to: {validated}")
    


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_df_synid",
        type=str,
        help="Synapse id for the input clinical file containing all the iatlas datasets",
    )
    parser.add_argument(
        "--features_df_synid",
        type=str,
        help="Synapse id for the features data for all the iatlas datasets",
    )
    parser.add_argument(
        "--cli_to_cbio_mapping_synid",
        type=str,
        help="Synapse id for the clinical to cbioportal mapping file",
    )
    parser.add_argument(
        "--cli_to_oncotree_mapping_synid",
        type=str,
        help="Synapse id for the clinical to oncotree mapping file",
    )
    parser.add_argument(
        "--output_folder_synid",
        type=str,
        help="Synapse id for output folder to store the export files"
    )
    parser.add_argument(
        "--datahub_tools_path",
        type=str,
        help="Path to datahub-study-curation-tools repo",
    )
    parser.add_argument(
        "--cbioportal_path",
        type=str,
        help="Path to cbioportal repo",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        default=False,
        help="Whether to run without saving to Synapse"
    )

    args = parser.parse_args()
    cli_to_cbio_mapping = get_cli_to_cbio_mapping(
        cli_to_cbio_mapping_synid=args.cli_to_cbio_mapping_synid
    )
    cli_df = preprocessing(
        input_df_synid=args.input_df_synid,
        features_df_synid=args.features_df_synid,
        cli_to_cbio_mapping=cli_to_cbio_mapping,
        cli_to_oncotree_mapping_synid=args.cli_to_oncotree_mapping_synid,
        datahub_tools_path=args.datahub_tools_path,
    )
    cli_dfs = split_into_patient_and_sample_data(
        input_data=cli_df, 
        cli_to_cbio_mapping=cli_to_cbio_mapping
    )
    for dataset in IATLAS_DATASETS:
        add_clinical_header(
            input_dfs=cli_dfs,
            dataset_name=dataset,
            datahub_tools_path=args.datahub_tools_path,
        )
        create_case_lists(
            clinical_file_name=f"{args.datahub_tools_path}/add-clinical-header/{dataset}/data_clinical_merged.txt",
            output_directory=f"{args.datahub_tools_path}/add-clinical-header/{dataset}/case_lists/",
            study_id=f"iatlas_{dataset}",
        )
        generate_meta_files(
            dataset_name=dataset, datahub_tools_path=args.datahub_tools_path
        )
        validate_export_files(
            input_df_synid=args.input_df_synid,
            dataset_name=dataset,
            datahub_tools_path=args.datahub_tools_path,
        )
        run_cbioportal_validator(
            dataset_name=dataset,
            cbioportal_path=args.cbioportal_path,
            datahub_tools_path=args.datahub_tools_path,
        )
        if not args.dry_run:
            save_to_synapse(
               dataset_name=dataset, 
               datahub_tools_path=args.datahub_tools_path, 
               output_folder_synid=args.output_folder_synid
            )

if __name__ == "__main__":
    main()

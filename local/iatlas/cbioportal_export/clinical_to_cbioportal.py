import argparse
from collections import defaultdict
import csv
import os
import subprocess
import sys

import pandas as pd
import synapseclient
import synapseutils


syn = synapseclient.login()
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
) -> pd.DataFrame:
    """_summary_

    Args:
        input_df_synid (str): _description_
        features_df_synid (str): _description_
        cli_to_cbio_mapping (pd.DataFrame): _description_
        cli_to_oncotree_mapping_synid (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    input_df = pd.read_csv(syn.get(input_df_synid), sep="\t")
    features_df = pd.read_csv(syn.get(features_df_synid), sep="\t", low_memory=True)
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
        "../datahub-study-curation-tools/add-clinical-header/cli_remapped.csv"
    )
    cli_w_cancer_types = convert_oncotree_codes()

    extra_cols = set(list(cli_w_cancer_types.columns)) - set(set(cli_to_cbio_mapping))
    cli_w_cancer_types = cli_w_cancer_types.drop(columns=list(extra_cols))
    return cli_remapped


def get_cli_to_cbio_mapping(cli_to_cbio_mapping_synid: str) -> pd.DataFrame:
    """_summary_

    Args:
        cli_to_cbio_mapping_synid (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    cli_to_cbio_mapping = pd.read_csv(syn.get(cli_to_cbio_mapping_synid).path, sep="\t")
    return cli_to_cbio_mapping


def get_updated_cli_attributes(cli_to_cbio_mapping: pd.DataFrame):
    """_summary_

    Args:
        cli_to_cbio_mapping_synid (str): _description_
        cli_attr_synid (str): _description_
    """
    cli_to_cbio_mapping = pd.read_csv(syn.get(cli_to_cbio_mapping).path, sep="\t")
    cli_attr = pd.read_csv(
        ".../datahub-study-curation-tools/add-clinical-header/clinical_attributes_metadata.txt".path,
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
    cli_attr_full.to_csv(
        "../datahub-study-curation-tools/add-clinical-header/clinical_attributes_metadata.txt",
        sep="\t",
    )


def convert_oncotree_codes() -> pd.DataFrame:
    """Converts the oncotree codes to CANCER_TYPE and CANCER_TYPE_DESCRIPTION

    Returns:
        pd.DataFrame: returns the converted clinical data with the new columns
    """

    cmd = f"""
    cd .../datahub-study-curation-tools/add-clinical-header/
    python3 ../datahub-study-curation-tools/oncotree-code-converter/oncotree_code_converter.py \
        --clinical-file cli_remapped.csv \''
    """
    # Run in shell to allow sourcing
    subprocess.run(cmd, shell=True, executable="/bin/bash")
    cli_w_cancer_types = pd.read_csv(
        "../datahub-study-curation-tools/add-clinical-header/cli_remapped.csv", sep="\t"
    )
    return cli_w_cancer_types


def add_clinical_header(input_df: pd.DataFrame, dataset_name: str) -> None:
    """Adds the clinical headers by calling cbioportal repo
    Args:
        cli_df (pd.DataFrame) - input clinical dataframe with all mappings
        dataset_name (str) - name of dataset to add clinical headers to

    """
    cli_df_subset = input_df[input_df["Dataset"] == dataset_name]

    dataset_dir = os.path.join(
        "/Users/rxu/datahub-study-curation-tools/add-clinical-header/", dataset_name
    )
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir)

    cli_df_subset.drop(columns=list(EXTRA_COLS)).to_csv(
        f"{dataset_dir}/data_clinical.txt", sep="\t", index=False
    )
    cmd = f"""
    cd .../datahub-study-curation-tools/add-clinical-header/
    python3 ../datahub-study-curation-tools/add-clinical-header/insert_clinical_metadata.py \
        -d {dataset_dir} \
        -n data_clinical_{dataset_name}.txt
    """

    # Run in shell to allow sourcing
    subprocess.run(cmd, shell=True, executable="/bin/bash")


def generate_meta_files(dataset_name: str) -> None:
    """Generates the meta* files for the given dataset

    Args:
        dataset_name (str): name of the iatlas dataset
    """
    dataset_dir = os.path.join(
        "/Users/rxu/datahub-study-curation-tools/add-clinical-header/", dataset_name
    )

    cmd = f"""
    python3 ../datahub-study-curation-tools/generate-meta-files/generate_meta_files.py \
        -d {dataset_dir} \
        -s iatlas_{dataset_name} \
        -m ../datahub-study-curation-tools/generate-meta-files/datatypes.txt
    """
    # Run in shell to allow sourcing
    subprocess.run(cmd, shell=True, executable="/bin/bash")


def create_case_lists_map(clinical_file_name):
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
    with open(clinical_file_name, "rU") as clinical_file:
        clinical_file_map = defaultdict(list)
        clin_samples = []
        reader = csv.DictReader(clinical_file, dialect="excel-tab")
        for row in reader:
            clinical_file_map[row["CANCER_TYPE"]].append(row["SAMPLE_ID"])
            clin_samples.append(row["SAMPLE_ID"])
    return clinical_file_map, clin_samples


def write_single_oncotree_case_list(cancer_type, ids, study_id, output_directory):
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


def write_case_list_all(clinical_samples, output_directory, study_id):
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


def write_case_list_files(clinical_file_map, output_directory, study_id):
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


def create_case_lists(clinical_file_name, output_directory, study_id):
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


def save_to_synapse(dataset_name: str) -> None:
    """Saves the dataset's clinical file, case lists 
        and meta files to its synapse respective folders

    Args:
        dataset_name (str): name of the iatlas dataset to save to
            synapse
    """
    # TODO: Make into argument
    iatlas_folder_synid = "syn64136279"
    dataset_dir = os.path.join(
        "../datahub-study-curation-tools/add-clinical-header/", dataset_name
    )
    # see if dataset_folder exists
    dataset_folder_exists = False
    for _, directory_names, _ in synapseutils.walk(syn=syn, synId=iatlas_folder_synid):
        directories = directory_names  # top level directories
        break

    for dataset_folder in directories:
        if dataset_name == dataset_folder[0]:
            dataset_folder_exists = True
            dataset_folder_id = dataset_folder[1]

    if not dataset_folder_exists:
        new_dataset_folder = synapseclient.Folder(
            dataset_name, parent=iatlas_folder_synid
        )
        dataset_folder_id = syn.store(new_dataset_folder).id

    # store clinical file
    syn.store(
        synapseclient.File(
            f"{dataset_dir}/data_clinical_{dataset_name}.txt",
            name="data_clinical.txt",
            parent=dataset_folder_id,
        )
    )
    # store meta* files
    syn.store(
        synapseclient.File(f"{dataset_dir}/meta_clinical.txt", parent=dataset_folder_id)
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


def validate_export_files(input_df: pd.DataFrame, dataset_name: str) -> None:
    """Does simple validation of the sample and patient count
        for the input and output clincial files

    Args:
        input_df (pd.DataFrame): input clinical file
        dataset_name (str): name of the iatlas dataset to validate
    """
    cli_df_subset = input_df[input_df["Dataset"] == dataset_name]
    dataset_dir = os.path.join(
        "../datahub-study-curation-tools/add-clinical-header/", dataset_name
    )

    output_df = pd.read_csv(
        os.path.join(dataset_dir, f"data_clinical_{dataset_name}.txt"),
        sep="\t",
        skiprows=5,
    )

    n_samples_start = len(cli_df_subset.sample_name.unique())
    n_patients_start = len(cli_df_subset.patient_name.unique())
    n_samples_end = len(output_df.SAMPLE_ID.unique())
    n_patients_end = len(output_df.PATIENT_ID.unique())

    print(dataset_name)
    if len(cli_df_subset) != len(output_df):
        print(f"Input is {len(cli_df_subset)} rows, output is {len(output_df)} rows")
    if n_samples_start != n_samples_end:
        print(
            f"There are {n_samples_start} samples start, there are {n_samples_end} samples end"
        )
    if n_patients_start != n_patients_end:
        print(
            f"There are {n_patients_start} patients start, there are {n_patients_end} patients end"
        )
    if len(output_df[output_df.duplicated()]) != 0:
        print("There are duplicates")
    print("\n\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_df_synid",
        type=str,
        help="Synapse id for the input clinical file containing all the iatlas datasets",
    )
    parser.add_argument(
        "features_df_synid",
        type=str,
        help="Synapse id for the features data for all the iatlas datasets",
    )
    parser.add_argument(
        "cli_to_cbio_mapping_synid",
        type=str,
        help="Synapse id for the clinical to cbioportal mapping file",
    )
    parser.add_argument(
        "cli_to_oncotree_mapping_synid",
        type=str,
        help="Synapse id for the clinical to oncotree mapping file",
    )
    
    args = parser.parse_args()
    
    input_df = pd.read_csv(syn.get(args.input_df_synid), sep="\t")
    cli_to_cbio_mapping = get_cli_to_cbio_mapping(
        cli_to_cbio_mapping_synid=args.cli_to_cbio_mapping_synid
    )
    cli_w_cancer_types = preprocessing(
        input_df_synid=input_df,
        features_df_synid=args.features_df_synid,
        cli_to_cbio_mapping=cli_to_cbio_mapping,
        cli_to_oncotree_mapping_synid=args.cli_to_oncotree_mapping_synid,
    )
    for dataset in IATLAS_DATASETS:
        add_clinical_header(input_df=cli_w_cancer_types, dataset_name=dataset)
        create_case_lists(
            clinical_file_name=f"../datahub-study-curation-tools/add-clinical-header/{dataset}/data_clinical.txt",
            output_directory=f"../datahub-study-curation-tools/add-clinical-header/{dataset}/case_lists/",
            study_id=f"iatlas_{dataset}",
        )
        generate_meta_files(dataset_name=dataset)
        save_to_synapse(dataset_name=dataset)

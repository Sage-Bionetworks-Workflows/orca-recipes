import argparse
from collections import defaultdict
import csv
import logging
from pathlib import Path
import os
import subprocess
import time
from typing import Dict, List

import pandas as pd
import synapseclient

import utils

syn = utils.synapse_login()

EXTRA_COLS = ["study_sample_name", "study_patient_name"]

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
    "Damrauer_NatComm_2022",
    "Rose_BrJCancer_2021",
    "Anders_JITC_2022",
    "Zappasodi_Nature_2021",
]

ONCOTREE_MERGE_COLS = ["TCGA_Study", "AMADEUS_Study", "Dataset"]

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

REQUIRED_OUTPUT_FILES = [
    "data_clinical_patient.txt",
    "data_clinical_sample.txt",
    "cases_sequenced.txt",
    "cases_all.txt",
    "meta_clinical_patient.txt",
    "meta_clinical_sample.txt",
]

def filter_out_non_analyses_samples(input_df: pd.DataFrame) -> pd.DataFrame:
    """Filter out the non analyses samples. 
        This is on a dataset by dataset basis.
        
        Here non-analyses samples are defined as DNA only tumor samples and 
        RNA samples not used in the analyses.

    Args:
        input_df (pd.DataFrame): input clinical data

    Returns:
        pd.DataFrame: output clinical data with the non analyses samples 
            filtered out
    """
    filtered_df = input_df[
        (~(input_df["SAMPLE_ID"].str.contains(r'-(?:nd|ad|nr)-', na=False)) & 
        (input_df["Dataset"]=="Anders_JITC_2022")) | (input_df["Dataset"]!="Anders_JITC_2022")
    ]
    return filtered_df


def remove_suffix_from_column_values(input_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Removes the attribute name suffix from all columns. Almost all values
    in the string columns in the clinical data has this format:
        [value]_[attribute_name]

    E.g: The Cancer_Tissue column has values like liver_cancer_tissue

    The exception is the AMADEUS_STUDY column which contains value with
    [value]_amadeus, this just needs special exception handling

    Args:
        input_df (pd.DataFrame): input clinical data with the attribute name
        formatted as a suffix into its values

    Returns:
        pd.DataFrame: cleaned clinical data without the suffixes attached
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    input_df_cleaned = input_df.copy()
    for col in input_df_cleaned.select_dtypes(include="object").columns:
        suffix = f"_{col}".lower()
        input_df_cleaned[col] = input_df_cleaned[col].str.replace(
            suffix, "", n=1, regex=False
        )
    # special scenario for AMADEUS_STUDY column
    if "AMADEUS_STUDY" in input_df_cleaned.columns:
        input_df_cleaned["AMADEUS_STUDY"] = input_df_cleaned[
            "AMADEUS_STUDY"
        ].str.replace("_amadeus", "", n=1, regex=False)

    # make sure we didn't create/reduce NA values
    if input_df.isna().sum().sum() != input_df_cleaned.isna().sum().sum():
        logger.error(
            "The number of NA values before and after removing the suffix doesn't match"
        )
    return input_df_cleaned


def update_case_of_column_values(
    input_df: pd.DataFrame, cli_to_cbio_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Each string column's values has an expected case: caps or
    titlecase that it is expected to be in. This function
    does the case-handling for those string columns,

    Args:
        input_df (pd.DataFrame): input clinical data
        cli_to_cbio_mapping (pd.DataFrame): mapping that
            also contains the expected case for each columns

    Returns:
        pd.DataFrame: case handled clinical data
    """
    input_df_cleaned = input_df.copy()
    for col in cli_to_cbio_mapping.NORMALIZED_HEADER.unique():
        case = cli_to_cbio_mapping[
            cli_to_cbio_mapping.NORMALIZED_HEADER == col
        ].Case.values[0]
        if case == "CAPS":
            input_df_cleaned[col] = (
                input_df_cleaned[col].str.replace("_", " ").str.upper()
            )
        elif case == "Title Case":
            input_df_cleaned[col] = (
                input_df_cleaned[col].str.replace("_", " ").str.title()
            )
        # no cleaning otherwise
        else:
            pass
    return input_df_cleaned


def remap_clinical_ids_to_paper_ids(input_df: pd.DataFrame) -> pd.DataFrame:
    """Remaps the clinical sample and patient id attributes to use the
        new paper identifiers that will be used across all data file types
        for consistency.

        This will also resolve the issue of hitting the max sample id character
        limit in cbioportal.

    Args:
        input_df (pd.DataFrame): input clinical dataset

    Returns:
        pd.DataFrame: remapped clinical dataset with new paper ids
    """
    cli_remapped = input_df.copy()
    cli_remapped.loc[~cli_remapped["study_sample_name"].isna(), "sample_name"] = (
        cli_remapped.loc[~cli_remapped["study_sample_name"].isna(), "study_sample_name"]
    )
    cli_remapped.loc[~cli_remapped["study_patient_name"].isna(), "patient_name"] = (
        cli_remapped.loc[
            ~cli_remapped["study_patient_name"].isna(), "study_patient_name"
        ]
    )
    cli_remapped.rename(
        columns={"sample_name": "SAMPLE_ID", "patient_name": "PATIENT_ID"}, inplace=True
    )
    return cli_remapped


def get_study_sample_name_to_lens_id_mapping(
    lens_id_mapping_synid: str, **kwargs
) -> pd.DataFrame:
    """Gets the mapping of the iatlas paper ids (study_sample_name) to
        lens id to merge into the clinical later

    Args:
        lens_id_mapping_synid (str): synapse id of the iatlas paper ids
            (study_sample_name) to lens id mapping

    Returns:
        pd.DataFrame: mapping of the iatlas paper ids to lens id
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    lens_id_mapping = pd.read_csv(syn.get(lens_id_mapping_synid).path, sep="\t")
    if lens_id_mapping.duplicated().any():
        logger.error(
            "There are duplicated rows in the study_sample_name to lens id mapping."
        )
    if lens_id_mapping.duplicated(subset=["study_sample_name"]).any():
        logger.error(
            "There are duplicated study_sample_names in the study_sample_name to lens id mapping."
        )

    if lens_id_mapping.duplicated(subset=["lens_id"]).any():
        logger.error(
            "There are duplicated lens_id in the study_sample_name to lens id mapping."
        )

    return lens_id_mapping


def add_lens_id_as_sample_display_name(
    input_df: pd.DataFrame, lens_id_mapping: pd.DataFrame, **kwargs
) -> pd.DataFrame:
    """Adds in lens_id as clinical attribute SAMPLE_DISPLAY_NAME
    NOTE: The lens maps are dataset specific so there is a possibility of SAMPLE_ID being of
    numeric data type so conversion to string is needed to merge with the clinical data where
    SAMPLE_ID is string data type

    Args:
        input_df (pd.DataFrame): input clinical data
        lens_id_mapping (pd.DataFrame): Mapping file of lens_id and study_sample_name

    Returns:
        pd.DataFrame: clinical data with SAMPLE_DISPLAY_NAME
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    lens_id_mapping_renamed = lens_id_mapping.rename(
        columns={"lens_id": "SAMPLE_DISPLAY_NAME", "study_sample_name": "SAMPLE_ID"}
    )
    # convert lens sample_id to string
    lens_id_mapping_renamed["SAMPLE_ID"] = lens_id_mapping_renamed["SAMPLE_ID"].astype(str)
    input_df_mapped = input_df.merge(
        lens_id_mapping_renamed, on=["SAMPLE_ID"], how="left"
    )
    if input_df_mapped.SAMPLE_DISPLAY_NAME.isnull().any():
        logger.error(
            "There are missing SAMPLE_DISPLAY_NAME (formerly lens_id) "
            "values after merging in lens_id on SAMPLE_ID"
        )
    return input_df_mapped


def merge_in_neoantigen_study_data(
    input_df : pd.DataFrame, 
    neoantigen_data_synid : str, **kwargs
    ) -> pd.DataFrame:
    """Adds in the new neoantigen summaries study data for the specific
        dataset to the overall clinical dataset (which contains all datasets)

    Args:
        input_df (pd.DataFrame): input clinical data
        neoantigen_data_synid (str): Synapse id to the neoantigen data

    Returns:
        pd.DataFrame: clinical data with neoantigen data added in
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    neoantigen_data = pd.read_csv(syn.get(neoantigen_data_synid).path, sep = "\t")
    neoantigen_data = neoantigen_data.rename(columns = {"Sample_ID":"SAMPLE_ID"})
    neoantigen_data['SAMPLE_ID'] = neoantigen_data['SAMPLE_ID'].astype(str)
    df_with_neoantigen = input_df.merge(
        neoantigen_data,
        how = "outer",
        on = "SAMPLE_ID"
    )
    if len(df_with_neoantigen) > len(input_df):
        logger.error(
            "There are more rows in the clinical data after merging in the neoantigen data."
        )
    return df_with_neoantigen


def preprocessing(
    input_df_synid: str,
    cli_to_cbio_mapping: pd.DataFrame,
    cli_to_oncotree_mapping_synid: str,
    neoantigen_data_synid : str,
    datahub_tools_path: str,
    **kwargs,
) -> pd.DataFrame:
    """Preprocesses the data, runs the individual steps:
        1. Gets the input clinical data
        2. Merges in the oncotree mappings
        3. Remaps the clinical sample and patient ids to use paper ids
        4. Merges in the neoantigen data
        5. Does some dataset specific filtering
        6. Remaps the columns to be cbioportal headers
        7. Converts the oncotree codes to have the CANCER_TYPE and CANCER_TYPE_DETAILED columns
        8. Updates the clinical_attributes_metadata.txt in prep for adding clinical headers

    Args:
        input_df_synid (str): Synapse id of input iatlas clinical dataset
        cli_to_cbio_mapping (pd.DataFrame): Clinical to cbioportal attirbutes mapping
        cli_to_oncotree_mapping_synid (str): Oncotree mapping for clinical dataset
        neoantigen_data_synid (str): Synapse id of the neoantigen dataset
        datahub_tools_path (str): Path to the datahub tools repo

    Returns:
        pd.DataFrame: preprocessed clinical merged dataset
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    input_df = pd.read_csv(syn.get(input_df_synid).path, sep="\t")
    cli_to_oncotree_mapping = pd.read_csv(
        syn.get(cli_to_oncotree_mapping_synid).path, sep="\t"
    )
    cli_with_oncotree = input_df.merge(
        cli_to_oncotree_mapping[ONCOTREE_MERGE_COLS + ["ONCOTREE_CODE"]],
        how="left",
        on=ONCOTREE_MERGE_COLS,
    )
    cli_remapped = remap_clinical_ids_to_paper_ids(input_df=cli_with_oncotree)
    cli_with_neoantigen = merge_in_neoantigen_study_data(
        input_df = cli_remapped, 
        neoantigen_data_synid = neoantigen_data_synid,
        logger = logger
    )
    cli_to_cbio_mapping_dict = dict(
        zip(
            cli_to_cbio_mapping["iATLAS_attribute"],
            cli_to_cbio_mapping["NORMALIZED_HEADER"],
        )
    )
    cli_remapped = cli_with_neoantigen.rename(columns=cli_to_cbio_mapping_dict)
    cli_remapped = filter_out_non_analyses_samples(cli_remapped)
    cli_remapped = remap_column_values(input_df=cli_remapped)
    cli_remapped_cleaned = remove_suffix_from_column_values(input_df=cli_remapped)
    cli_remapped_cleaned = update_case_of_column_values(
        input_df=cli_remapped_cleaned, cli_to_cbio_mapping=cli_to_cbio_mapping
    )
    cli_remapped_cleaned.to_csv(
        f"{datahub_tools_path}/add-clinical-header/cli_remapped.csv",
        index=False,
        sep="\t",
        float_format="%.12g",
    )
    cli_w_cancer_types = convert_oncotree_codes(datahub_tools_path)
    get_updated_cli_attributes(cli_to_cbio_mapping, datahub_tools_path)

    return cli_w_cancer_types


def split_into_patient_and_sample_data(
    input_data: pd.DataFrame, cli_to_cbio_mapping: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """This splits the preprocessed clinical dataset (prior to adding clinical headers)
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
        "patient": input_data[patient_cols + ["Dataset"]].drop_duplicates(),
        "sample": input_data[sample_cols + ["Dataset"]],
    }


def remap_column_values(input_df: pd.DataFrame) -> pd.DataFrame:
    """Remaps the column values into the accepted cbioportal
        values.
        Current columns that get remapped:
            - OS_STATUS
            - PFS_STATUS
        Checks that the columns have been remapped otherwise
        throws an error
    Args:
        input_df (pd.DataFrame): input data to be remapped

    Returns:
        pd.DataFrame: output data with columns remapped
    """
    remapped_df = input_df.copy()
    mapping = {0: "0:LIVING", 1: "1:DECEASED"}
    cols_to_remap = ["OS_STATUS", "PFS_STATUS"]

    remapped_df[cols_to_remap] = remapped_df[cols_to_remap].replace(mapping)
    return remapped_df


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
        sep="\t"
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
        f"{datahub_tools_path}/add-clinical-header/clinical_attributes_metadata.txt",
        sep="\t",
        float_format="%.12g",
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


def rename_files_on_disk(filepath : str) -> None:
    """Renames files on disk by removing the .metadata ext from filenames.
        NOTE: This will overwrite previous files with the same name.
        
        This is needed because the insert_clinical_metadata script from
        datahub-curation-tools saves the sample and patient files with
        ".metadata" ext but the cbioportal validation tool expects them to be 
        withou the ".metadata"

    Args:
        filepath (str): the filepath to remove the .metadata ext from
    """
    filepath_new = filepath.removesuffix(".metadata")
    os.replace(filepath, filepath_new)
    
    
def get_all_non_na_columns(input_df: pd.DataFrame) -> List[str]:
    """Gets all the columns in input data without all (100%) NAs
    Args:
        input_df (pd.DataFrame): input data

    Returns:
        List[str]: Returns a list of column names in df where there is at least
        one value (subsets out columns with all NAs)
    """
    return input_df.columns[~input_df.isna().all()].tolist()


def add_clinical_header(
    input_dfs: Dict[str, pd.DataFrame],
    dataset_name: str,
    datahub_tools_path: str,
) -> None:
    """Adds the clinical headers to the patient and sample data
        by calling cbioportal repo

    Args:
        input_dfs (Dict[str, pd.DataFrame]): A dictionary with the following keys:
            "merged" : the clinical merged file before split
            "patient" : patient dataset
            "sample" : sample dataset
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

    sample_df_subset = input_dfs["sample"][
        input_dfs["sample"]["Dataset"] == dataset_name
    ]

    # get the columns without 100% NAs
    patient_subset_cols = get_all_non_na_columns(input_df=patient_df_subset)
    sample_subset_cols = get_all_non_na_columns(input_df=sample_df_subset)

    # saves the patient and sample files without pandas float
    sample_df_subset[sample_subset_cols].drop(columns=["Dataset"]).to_csv(
        f"{dataset_dir}/data_clinical_sample.txt",
        sep="\t",
        index=False,
        float_format="%.12g",
    )

    patient_df_subset[patient_subset_cols].drop(columns=["Dataset"]).to_csv(
        f"{dataset_dir}/data_clinical_patient.txt",
        sep="\t",
        index=False,
        float_format="%.12g",
    )
    cmd = f"""
    cd {datahub_tools_path}/add-clinical-header/
    python3 {datahub_tools_path}/add-clinical-header/insert_clinical_metadata.py \
        -d {dataset_dir}    
    """
    time.sleep(2) # give subprocess some time before checking
    subprocess.run(cmd, shell=True, executable="/bin/bash")
    time.sleep(2) # give subprocess some time before checking
    
    # remove .metadata from files
    rename_files_on_disk(filepath = f"{dataset_dir}/data_clinical_patient.txt.metadata")
    rename_files_on_disk(filepath = f"{dataset_dir}/data_clinical_sample.txt.metadata")

    # saved merged for case lists
    merged_df_subset = input_dfs["merged"][
        input_dfs["merged"]["Dataset"] == dataset_name
    ]
    merged_df_subset.drop(columns=["Dataset"]).to_csv(
        f"{dataset_dir}/data_clinical_merged.txt",
        sep="\t",
        index=False,
        float_format="%.12g",
    )


def generate_meta_files(dataset_name: str, datahub_tools_path: str) -> None:
    """Generates the meta* files for the given dataset:
        Here we generate meta files for clinical, patient and the
        study as a whole.

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

    # create meta_study file
    metadata = [
        f"cancer_study_identifier: iatlas_{dataset_name}\n",
        "type_of_cancer: mixed\n",
        "name: TBD\n",
        "pmid: 29033130\n",
        "reference_genome: hg38\n",
        "citation: PLACEHOLDER\n",
        "description: PLACEHOLDER\n",
    ]
    with open(f"{dataset_dir}/meta_study.txt", "w") as meta_file:
        meta_file.write("".join(metadata))


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


def create_case_lists(
    clinical_file_name: str,
    output_directory: str,
    study_id: str,
) -> None:
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


def validate_export_files(
    input_df_synid: str, dataset_name: str, datahub_tools_path: str, **kwargs
) -> None:
    """Does simple validation of the sample and patient count
        for the input and output clincial files

        Validation rules available:
        -  Validation #1: Checks that all REQUIRED_OUTPUT_FILES are present locally
        -  Validation #2: Checks that rows match before and after
        -  Validation #3: Checks that samples match before and after
        -  Validation #4: Checks that patients match before and after
        -  Validation #5: Checks that there are no NA sample values
        -  Validation #6: Checks that there are no NA patient values

    Args:
        input_df_synid (str): input clinical file synapse id
        dataset_name (str): name of the iatlas dataset to validate
        datahub_tools_path (str): Path to the datahub tools repo
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    input_df = pd.read_csv(syn.get(input_df_synid).path, sep="\t")
    cli_df_subset = input_df[input_df["Dataset"] == dataset_name]
    dataset_dir = utils.get_local_dataset_output_folder_path(
        dataset_name, datahub_tools_path
    )
    for file in REQUIRED_OUTPUT_FILES:
        if file.startswith("cases"):
            required_file_path = f"{dataset_dir}/case_lists/{file}"
        else:
            required_file_path = f"{dataset_dir}/{file}"
        if not Path(required_file_path).exists():
            logger.error(f"Missing REQUIRED OUTPUT FILE: {required_file_path}")

    output_patient_df = pd.read_csv(
        os.path.join(dataset_dir, f"data_clinical_patient.txt"),
        sep="\t",
        skiprows=4,  # skips the clinical header when reading it in
    )

    output_samples_df = pd.read_csv(
        os.path.join(dataset_dir, f"data_clinical_sample.txt"),
        sep="\t",
        skiprows=4,  # skips the clinical header when reading it in
    )
    n_samples_start = len(cli_df_subset.sample_name.unique())
    n_patients_start = len(cli_df_subset.patient_name.unique())
    n_samples_end = len(output_samples_df.SAMPLE_ID.unique())
    n_patients_end = len(output_patient_df.PATIENT_ID.unique())

    if len(cli_df_subset) != len(output_samples_df):
        logger.error(
            f"Input is {len(cli_df_subset)} rows, output is {len(output_samples_df)} rows"
        )
    if n_samples_start != n_samples_end:
        logger.error(
            f"There are {n_samples_start} samples start, there are {n_samples_end} samples end"
        )
    if n_patients_start != n_patients_end:
        logger.error(
            f"There are {n_patients_start} patients start, there are {n_patients_end} patients end"
        )
    if output_samples_df.SAMPLE_ID.isna().any():
        logger.error("There are missing SAMPLE_ID values.")

    if output_patient_df.PATIENT_ID.isna().any():
        logger.error("There are missing PATIENT_ID values.")

    # check that there are no all NA columns
    if output_patient_df.isna().all().any():
        logger.error("There are patient columns with ALL NAs.")

    if output_samples_df.isna().all().any():
        logger.error("There are sample columns with ALL NAs.")

    print("\n\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        nargs="+",
        default=IATLAS_DATASETS,
        help="List of dataset names to run for. Optional. Defaults to IATLAS_DATASETS global variable.",
    )
    parser.add_argument(
        "--input_df_synid",
        type=str,
        help="Synapse id for the input clinical file containing all the iatlas datasets",
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
        "--lens_id_mapping_synid",
        type=str,
        help="Synapse id for the study_sample_name (paper ids) to lens id mapping file. Optional. Defaults to None, then adding lens id mapping is skipped",
        default=None
    )
    parser.add_argument(
        "--neoantigen_data_synid",
        type=str,
        help="Synapse id for the summary neoantigen data that needs to be merged into the clinical.",
    )
    parser.add_argument(
        "--datahub_tools_path",
        type=str,
        help="Path to datahub-study-curation-tools repo",
    )
    parser.add_argument(
        "--clear_workspace",
        action="store_true",
        default=False,
        help="Whether to clear local directory of files or not",
    )

    args = parser.parse_args()
    if args.clear_workspace:
        utils.clear_workspace(dir_path=f"{args.datahub_tools_path}/add-clinical-header")

    # general logger
    main_logger = utils.create_logger(
        dataset_name=None,
        datahub_tools_path=args.datahub_tools_path,
        log_file_name="iatlas_general_cli_log.txt",
    )
    cli_to_cbio_mapping = get_cli_to_cbio_mapping(
        cli_to_cbio_mapping_synid=args.cli_to_cbio_mapping_synid
    )
    cli_df = preprocessing(
        input_df_synid=args.input_df_synid,
        cli_to_cbio_mapping=cli_to_cbio_mapping,
        cli_to_oncotree_mapping_synid=args.cli_to_oncotree_mapping_synid,
        neoantigen_data_synid=args.neoantigen_data_synid,
        datahub_tools_path=args.datahub_tools_path,
        logger = main_logger,
    )
    cli_dfs = split_into_patient_and_sample_data(
        input_data=cli_df, cli_to_cbio_mapping=cli_to_cbio_mapping
    )
    # Skips the lens mapping when not provided because it's technically optional
    if args.lens_id_mapping_synid is not None:
        lens_id_mapping = get_study_sample_name_to_lens_id_mapping(
            lens_id_mapping_synid=args.lens_id_mapping_synid, logger=main_logger
        )
        cli_dfs["sample"] = add_lens_id_as_sample_display_name(
            input_df=cli_dfs["sample"],
            lens_id_mapping=lens_id_mapping,
            logger=main_logger,
        )
    for dataset in args.dataset:
        dataset_flagger = utils.ErrorFlagHandler()
        dataset_logger = utils.create_logger(
            dataset_name=dataset,
            datahub_tools_path=args.datahub_tools_path,
            log_file_name="iatlas_cli_validation_log.txt",
            flagger=dataset_flagger
        )
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
            logger=dataset_logger,
        )
        if dataset_flagger.had_error:
            dataset_logger.error("FAILED: Validation of study failed")


if __name__ == "__main__":
    main()

import argparse
import logging
import os
import subprocess
from typing import Dict

import pandas as pd

import utils

syn = utils.synapse_login()


REQUIRED_MAF_COLS = [
    "Hugo_Symbol",
    "Entrez_Gene_Id",
    "Center",
    "NCBI_Build",
    "Chromosome",
    "Start_Position",
    "End_Position",
    "Strand",
    "Consequence",
    "Variant_Classification",
    "Variant_Type",
    "Reference_Allele",
    "Tumor_Seq_Allele1",
    "Tumor_Seq_Allele2",
    "dbSNP_RS",
    "dbSNP_Val_Status",
    "Tumor_Sample_Barcode",
    "Matched_Norm_Sample_Barcode",
    "Match_Norm_Seq_Allele1",
    "Match_Norm_Seq_Allele2",
    "Tumor_Validation_Allele1",
    "Tumor_Validation_Allele2",
    "Match_Norm_Validation_Allele1",
    "Match_Norm_Validation_Allele2",
    "Verification_Status",
    "Validation_Status",
    "Mutation_Status",
    "Sequencing_Phase",
    "Sequence_Source",
    "Validation_Method",
    "Score",
    "BAM_File",
    "Sequencer",
    "n_ref_count",
    "n_alt_count",
    "HGVSc",
    "HGVSp",
    "HGVSp_Short",
    "Transcript_ID",
    "RefSeq",
    "Protein_position",
    "Codons",
    "Exon_Number",
    "AA_AF",
    "AF",
    "AFR_AF",
    "ALLELE_NUM",
    "AMR_AF",
    "ASN_AF",
    "Allele",
    "Amino_acids",
    "BIOTYPE",
    "CANONICAL",
    "CCDS",
    "CDS_position",
    "CLIN_SIG",
    "DISTANCE",
    "DOMAINS",
    "EAS_AF",
    "EA_AF",
    "ENSP",
    "EUR_AF",
    "EXON",
    "Existing_variation",
    "FILTER",
    "Feature",
    "Feature_type",
    "GENE_PHENO",
    "Gene",
    "HGNC_ID",
    "HGVS_OFFSET",
    "HIGH_INF_POS",
    "IMPACT",
    "INTRON",
    "MINIMISED",
    "MOTIF_NAME",
    "MOTIF_POS",
    "MOTIF_SCORE_CHANGE",
    "PHENO",
    "PICK",
    "PUBMED",
    "PolyPhen",
    "SAS_AF",
    "SIFT",
    "SOMATIC",
    "STRAND_VEP",
    "SWISSPROT",
    "SYMBOL",
    "SYMBOL_SOURCE",
    "TREMBL",
    "TSL",
    "UNIPARC",
    "VARIANT_CLASS",
    "all_effects",
    "cDNA_position",
    "flanking_bps",
    "genomic_location_explanation",
    "gnomADe_AF",
    "gnomADe_AFR_AF",
    "gnomADe_AMR_AF",
    "gnomADe_ASJ_AF",
    "gnomADe_EAS_AF",
    "gnomADe_FIN_AF",
    "gnomADe_NFE_AF",
    "gnomADe_OTH_AF",
    "gnomADe_SAS_AF",
    "n_depth",
    "t_depth",
    "t_ref_count",
    "t_alt_count",
    "vcf_id",
    "vcf_pos",
    "vcf_qual",
    "Annotation_Status",
    "Peptide",
    "HLA_Allele",
    "MHCflurry_2.1.1_affinity_nm",
    "MHCflurry_2.1.1_presentation_score"
]

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
    
    # set both to string to standardize
    neoantigen_data["Sample_ID"] = neoantigen_data["Sample_ID"].astype(str)
    input_df["Tumor_Sample_Barcode"] = input_df["Tumor_Sample_Barcode"].astype(str)
    
    if set(input_df["Tumor_Sample_Barcode"].unique()) != set(
        neoantigen_data["Sample_ID"].unique()
    ):
        logger.error(
            "The Tumor_Sample_Barcode values in the maf data do not match the Sample_ID values in the neoantigen data."
        )

def validate_that_required_columns_are_present(
    input_df: pd.DataFrame, dataset_file_name : str, required_cols : list, **kwargs
) -> None:
    """Validate that required set of columns are present

    Args:
        input_df (pd.DataFrame): input dataset
        dataset_file_name (str): name of the dataset file
        required_cols (list): list of the required columns
    """
    logger = kwargs.get("logger", logging.getLogger(__name__))
    if set(required_cols) != set(list(input_df.columns)):
        missing_cols = set(required_cols) - set(list(input_df.columns))
        logger.error(f"Missing required columns in {dataset_file_name}: {list(missing_cols)}")


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
    # exclude the validator
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
    validate_that_required_columns_are_present(
        input_df = all_files["data_mutations.txt"], 
        dataset_file_name="data_mutations.txt",
        required_cols = REQUIRED_MAF_COLS,
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

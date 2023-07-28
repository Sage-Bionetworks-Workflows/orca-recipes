#!/usr/bin/env python3

import sys
from typing import List

import pandas as pd
import synapseclient
from synapseclient import File


def syn_login():
    syn = synapseclient.Synapse()
    syn.login()
    return syn


def download_data_files(parent: str, syn: synapseclient.Synapse):
    """downloads all TSV files in parent synapse folder from synapse id provided"""
    children = syn.getChildren(parent=parent)
    file_entity_list = []
    for child in children:
        if child["name"].endswith(".tsv"):
            file = syn.get(child["id"])
            file_entity_list.append(file)
    return file_entity_list


def get_patient_id(map_file_id: str, file_id: str, syn: synapseclient.Synapse) -> str:
    """Swaps out the file data id with the biospecimen id from the mapping table"""
    map_file = syn.get(map_file_id)
    mapping_df = pd.read_csv(map_file.path)
    biospecimen_id = mapping_df.loc[
        mapping_df["HTAN_Data_File_ID"] == file_id, "HTAN_Assayed_Biospecimen_ID"
    ].values[0]
    return biospecimen_id


def transform_df(df: pd.DataFrame, patient_id: str) -> pd.DataFrame:
    """Applies needed transformations to each component dataframe prior to merging"""
    # Grab Hugo ID from target_id col
    df.insert(0, "Hugo", df["target_id"].str.split("|", expand=True)[5])
    # Only need Hugo ID and tpm value
    df = df[["Hugo", "tpm"]]
    # Only need max tpm value for each Hugo ID
    idx = df.groupby("Hugo")["tpm"].idxmax()
    df = df.loc[idx]
    # Rename tpm to the patient_id from the file name for merging into master data sheet
    df = df.rename(columns={"tpm": patient_id})
    return df


def load_data_files(
    file_entity_list: List[synapseclient.File],
    map_file_id: str,
    syn: synapseclient.Synapse,
) -> List[pd.DataFrame]:
    """loads all files in data_diectory into dataframes, creates Hugo column,
    grabs Hugo and tpm columns, renames tppm column to patient id, appends each df to gene_df_list
    """
    gene_df_list = []
    for file in file_entity_list:
        file_id = file.annotations.get("HTANDataFileID")[0]
        patient_id = get_patient_id(map_file_id=map_file_id, file_id=file_id, syn=syn)
        df = pd.read_csv(file.path, sep="\t")
        gene_df = transform_df(df=df, patient_id=patient_id)
        gene_df_list.append(gene_df)
        print(f"Data from {file.name} loaded")
    return gene_df_list


def merge_and_export(
    gene_df_list: List[pd.DataFrame],
    export_name: str,
) -> pd.DataFrame:
    """Joins all dfs in gene_df_list by Hugo column, exports final_df to csv, returns name of file"""
    final_df = gene_df_list[0]
    gene_df_list.pop(0)
    for gene_df in gene_df_list:
        final_df = pd.merge(final_df, gene_df, on="Hugo", how="outer")
    print("dfs all joined into final_df")
    final_df.to_csv(export_name, index=False, sep="\t")
    return export_name


def syn_upload(
    export_name: str,
    file_entity_list: list,
    map_file_id: str,
    upload_location: str,
    syn: synapseclient.Synapse,
):
    """Uploads exported data file to Synapse in provided location"""
    file = File(
        export_name,
        parent=upload_location,
    )
    provenance = [f.id for f in file_entity_list]
    provenance.append(map_file_id)
    file = syn.store(
        file,
        used=provenance,
        executed=[
            "https://raw.githubusercontent.com/Sage-Bionetworks-Workflows/iatlas-scripts/immune_subtype_classifier/prepare_data_sheet.py"
        ],
        forceVersion=False,
    )
    print(f"{export_name} uploaded to Synapse in {upload_location}")


def main():
    parent = sys.argv[1]  # "syn26535390"
    map_file_id = sys.argv[2]  # "syn51526489"
    export_name = sys.argv[3]  # "immune_subtype_sample_sheet.tsv"
    upload_location = sys.argv[4]  # "syn51526284"
    syn = syn_login()
    file_entity_list = download_data_files(parent=parent, syn=syn)
    gene_df_list = load_data_files(
        file_entity_list=file_entity_list, map_file_id=map_file_id, syn=syn
    )
    export_name = merge_and_export(gene_df_list=gene_df_list, export_name=export_name)
    syn_upload(
        export_name=export_name,
        file_entity_list=file_entity_list,
        map_file_id=map_file_id,
        upload_location=upload_location,
        syn=syn,
    )


if __name__ == "__main__":
    main()

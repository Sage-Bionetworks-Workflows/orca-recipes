import argparse
import os
import subprocess
import sys

import synapseclient
import synapseutils

import utils

syn = utils.synapse_login()

def write_case_lists_all_and_sequenced(
    dataset_name: str, datahub_tools_path: str, study_id: str
) -> None:
    """Adds the case lists for all samples and sequenced samples
        using cbioportal tools. This needs to be done together.
        Sequenced samples contains the subset of samples in
        mutation data that are in the clinical samples.

    Args:
        dataset_name (str): name of dataset to add clinical headers to
        datahub_tools_path (str): Path to the datahub tools repo
        study_id (str): cBioPortal study id
    """
    dataset_dir = utils.get_local_dataset_output_folder_path(
        dataset_name, datahub_tools_path
    )
    cmd = f"""
    python3 {datahub_tools_path}/generate-case-lists/generate_case_lists.py \
        -c {datahub_tools_path}/generate-case-lists/case_list_conf.txt \
        -d {dataset_dir}/case_lists \
        -s {dataset_dir} \
        -i {study_id}
    """
    subprocess.run(cmd, shell=True, executable="/bin/bash")
    

def save_to_synapse(
    dataset_name: str,
    datahub_tools_path: str,
    output_folder_synid: str,
    version_comment: str = None,
) -> None:
    """Saves the dataset's clinical file, case lists
        and meta files to its synapse respective folders

    Args:
        dataset_name (str): name of the iatlas dataset to save to
            synapse
        datahub_tools_path (str): Path to the datahub tools repo
        output_folder_synid (str): Synapse id of the output folder
        version_comment (str): Version comment for this iteration of files on synapse. Optional.
            Defaults to None.
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

    # store required files
    for file in utils.REQUIRED_OUTPUT_FILES:
        syn.store(
            synapseclient.File(
                f"{dataset_dir}/{file}",
                name=file,
                parent=dataset_folder_id,
                version_comment=version_comment
            )
        )
    
    # store case lists
    case_list_files = os.listdir(os.path.join(dataset_dir, "case_lists"))
    case_list_folder = synapseclient.Folder("case_lists", parent=dataset_folder_id)
    try:
        case_list_folder_id = syn.store(case_list_folder).id
    except:
        sys.exit(-1)
    for file in case_list_files:
        syn.store(
            synapseclient.File(
                f"{dataset_dir}/case_lists/{file}",
                parent=case_list_folder_id,
                version_comment=version_comment,
            )
        )
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=str,
        help="Name of dataset to run processing for",
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
        "--create_case_lists",
        action="store_true",
        default=False,
        help="Whether to generate the all and sequenced caselists",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        default=False,
        help="Whether to run without saving to Synapse",
    )
    parser.add_argument(
        "--version_comment",
        default=None,
        type=str,
        help="Version comment for the files on Synapse. Optional. Defaults to None.",
    )
    args = parser.parse_args()
    if args.create_case_lists:
        write_case_lists_all_and_sequenced(
            dataset_name=args.dataset, 
            datahub_tools_path=args.datahub_tools_path,
            study_id=f"iatlas_{args.dataset}",
        )
    if args.upload:
        save_to_synapse(
            dataset_name=args.dataset,
            datahub_tools_path=args.datahub_tools_path,
            output_folder_synid=args.output_folder_synid,
            version_comment=args.version_comment,
        )


if __name__ == "__main__":
    main()
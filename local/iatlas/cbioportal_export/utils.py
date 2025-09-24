import logging
import os
import shutil
import sys
import typing

import pandas as pd
import synapseclient


REQUIRED_OUTPUT_FILES = [
    "data_clinical_patient.txt",
    "data_clinical_sample.txt",
    "meta_clinical_patient.txt",
    "meta_clinical_sample.txt",
    "data_mutations.txt",
    "meta_mutations.txt",
    "data_gene_signatures.txt",
    "meta_gene_signatures.txt",
    "data_rna_seq_mrna.txt",
    "meta_rna_seq_mrna.txt",
]


def synapse_login(debug: typing.Optional[bool] = False) -> synapseclient.Synapse:
    """
    Logs into Synapse if credentials are saved.
    If not saved, then user is prompted username and auth token.

    Args:
        debug: Synapse debug feature. Defaults to False

    Returns:
        Synapseclient object
    """
    # If debug is True, then silent should be False
    silent = False if debug else True
    syn = synapseclient.Synapse(
        debug=debug, silent=silent, user_agent=f"iatlas-cbioportal/0.0.0"
    )
    try:
        syn.login()
    except Exception as ex:
        raise ValueError(
            "Please view https://help.synapse.org/docs/Client-Configuration.1985446156.html"
            "to configure authentication to the client.  Configure a ~/.synapseConfig"
            "or set the SYNAPSE_AUTH_TOKEN environmental variable."
        ) from ex
    return syn


class ErrorFlagHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.had_error = False

    def emit(self, record):
        self.had_error = True


def create_logger(
    dataset_name: str,
    datahub_tools_path: str,
    log_file_name: str,
    flagger: logging.Handler = None,
) -> logging.Logger:
    """This creates a logger for the current dataset and process

    Args:
        dataset_name (str): Name of the dataset
        datahub_tools_path (str): Path to the datahub tools path repo
        log_file_name (str): Name of the log file
        flagger (logging.Handler): The error handler for the logger

    Returns:
        logging.Logger: logger for use in the processing functions
    """
    if dataset_name:
        dataset_dir = get_local_dataset_output_folder_path(
            dataset_name, datahub_tools_path
        )
    else:
        dataset_dir = datahub_tools_path
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    file_handler = logging.FileHandler(f"{dataset_dir}/{log_file_name}", mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )

    logger.handlers = [stdout_handler, file_handler]
    if flagger:
        logger.addHandler(flagger)
    logger.info(f"Log file saved to: {dataset_dir}/{log_file_name}")
    return logger


def clear_workspace(dir_path: str) -> None:
    """Clears all the folders under a directory

    Args:
        dir_path (str): directory path
    """
    shutil.rmtree(dir_path)
    os.makedirs(dir_path, exist_ok=True)


def get_local_dataset_output_folder_path(
    dataset_name: str, datahub_tools_path: str
) -> str:
    """Gets the filepath of the local folder that all the dataset
        specific files will be saved to

    Args:
        dataset_name (str): Name of the dataset
        datahub_tools_path (str): Path to the datahub tools repo

    Returns:
        str: path to local folder
    """
    dataset_dir = os.path.join(
        f"{datahub_tools_path}/add-clinical-header/", dataset_name
    )
    return dataset_dir


def remove_pandas_float(df: pd.DataFrame, header: bool = True) -> str:
    """Remove decimals (ending in .0) from float values due to
    pandas behavior that converts integer values to decimal values
    in mixed dtype data frames in tsv and csv files prior to saving them.

    Args:
        df (pd.DataFrame): input data
        header (bool, optional): Whether there is a header or not.
            Defaults to True.

    Returns:
        str: data frame in text form with .0 replaced
    """
    if header:
        text = df.to_csv(sep="\t", index=False)
    else:
        text = df.to_csv(sep="\t", index=False, header=None)

    text_replaced = text.replace(".0\t", "\t")
    text_replaced = text_replaced.replace(".0\n", "\n")
    return text_replaced

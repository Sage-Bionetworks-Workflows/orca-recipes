import logging
import os
import shutil
import sys

import pandas as pd


def create_logger(
    dataset_name: str, datahub_tools_path: str, log_file_name: str
) -> logging.Logger:
    """This creates a logger for the current dataset and process

    Args:
        dataset_name (str): Name of the dataset
        datahub_tools_path (str): Path to the datahub tools path repo
        log_file_name (str): Name of the log file

    Returns:
        logging.Logger: logger for use in the processing functions
    """
    dataset_dir = get_local_dataset_output_folder_path(dataset_name, datahub_tools_path)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    file_handler = logging.FileHandler(
        f"{dataset_dir}/{log_file_name}", mode="w"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )

    logger.handlers = [stdout_handler, file_handler]
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

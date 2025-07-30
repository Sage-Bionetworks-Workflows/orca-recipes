import os
import pytest
import pandas as pd
import tempfile

import utils

def test_that_clear_workspace_removes_subdirectories():
    # Create a temporary parent directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # create some subdirectories
        subdirs = ["folder1", "folder2", "folder3"]
        for sub in subdirs:
            os.makedirs(os.path.join(temp_dir, sub))
            
        # Create some files
        filenames = ["file1.txt", "file2.txt"]
        for fname in filenames:
            with open(os.path.join(temp_dir, fname), "w") as f:
                f.write("test")

        assert set(os.listdir(temp_dir)) == set(subdirs + filenames)

        utils.clear_workspace(temp_dir)

        # Check that all files and subfolders are removed
        assert os.listdir(temp_dir) == []
        

def test_that_clear_workspace_does_nothing_on_empty_directory():
    with tempfile.TemporaryDirectory() as temp_dir:
        # Make sure the directory is empty
        assert os.listdir(temp_dir) == []

        utils.clear_workspace(temp_dir)

        # Still should be empty (not deleted or modified)
        assert os.listdir(temp_dir) == []


@pytest.mark.parametrize(
    "dataset_name, datahub_tools_path, expected",
    [
        (
            "genie_dataset",
            "/home/user/datahub-tools",
            "/home/user/datahub-tools/add-clinical-header/genie_dataset",
        ),
        (
            "genie_dataset",
            "/home/user/datahub-tools/",
            "/home/user/datahub-tools//add-clinical-header/genie_dataset",
        ),
        (
            "genie_dataset",
            "datahub-tools",
            "datahub-tools/add-clinical-header/genie_dataset",
        ),
    ],
    ids=["normal_path", "path_with_trailing_slash", "relative_path"],
)
def test_get_local_dataset_output_folder_path(
    dataset_name, datahub_tools_path, expected
):
    result = utils.get_local_dataset_output_folder_path(
        dataset_name, datahub_tools_path
    )
    assert result == expected


@pytest.mark.parametrize("header", [True, False])
def test_that_remove_pandas_float_removes_dot_zero(header):
    df = pd.DataFrame({
        "col1": [1.0, 2.0, 3.0],
        "col2": ["a", "b", "c"]
    })
    output = utils.remove_pandas_float(df, header=header)

    # .0 should be removed from float values
    assert ".0\t" not in output
    assert ".0\n" not in output
    # Confirm that the resulting text still contains the correct values
    for i in range(1, 4):
        assert f"{i}\t" in output or f"{i}\n" in output


@pytest.mark.parametrize("header", [True, False])
def test_that_remove_pandas_float_keeps_true_decimals(header):
    df = pd.DataFrame({
        "col1": [1.5, 2.25, 3.0],
        "col2": ["x", "y", "z"]
    })
    output = utils.remove_pandas_float(df, header=header)

    # Decimal values that are not .0 should remain untouched
    assert "1.5" in output
    assert "2.25" in output
    assert "3" in output


def test_that_remove_pandas_float_does_nothing_to_empty_dataframe():
    df = pd.DataFrame(columns=["col1", "col2"])
    output = utils.remove_pandas_float(df)
    assert "col1" in output
    assert "col2" in output

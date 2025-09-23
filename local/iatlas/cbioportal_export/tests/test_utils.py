import os
import pytest
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


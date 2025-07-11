# test_clinical_pipeline.py
import csv
import os
import tempfile
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import clinical_to_cbioportal as cli_to_cbio


@pytest.fixture
def test_input_clinical_data():
    input_data = pd.DataFrame({
        "PATIENT_ID": ["P1", "P2"],
        "SAMPLE_ID": ["S1", "S2"],
        "CANCER_TYPE": ["TypeA", "TypeB"],
        "CANCER_TYPE_DETAILED": ["SubtypeA", "SubtypeB"],
        "AGE": [55, 60],
        "STAGE": ["II", "III"],
        "TREATMENT": ["Chemo", "Radiation"],
        "Dataset":["SAGE-1", "SAGE-2"]
    })
    return input_data


@pytest.fixture
def test_cli_to_cbio_mapping():
    cli_to_cbio_mapping = pd.DataFrame({
        "ATTRIBUTE_TYPE": ["PATIENT", "SAMPLE", "SAMPLE"],
        "NORMALIZED_HEADER": ["AGE", "STAGE", "TREATMENT"]
    })
    return cli_to_cbio_mapping
    
@pytest.mark.parametrize(
    "input, expected",
    [
        (pd.DataFrame(
            {
                "PRIORITY":[1.0, None, float('nan')]
            }
        ),
        pd.DataFrame(
            {
                "PRIORITY":["1", None, float('nan')]
            }
        )),
        (pd.DataFrame(
            {
                "PRIORITY":[1, 1, 1]
            }
        ),
        pd.DataFrame(
            {
                "PRIORITY":["1", "1", "1"]
            }
        )),
        (pd.DataFrame(
            {
                "PRIORITY":[None, None, None]
            }
        ),
        pd.DataFrame(
            {
                "PRIORITY":[None, None, None]
            }
        ))
    ],
    ids = ["mixed_dtype", "all_integers", "no_floats"]
)
def test_that_convert_floats_in_priority_column_converts_correctly(input, expected):
    output = cli_to_cbio.convert_floats_in_priority_column(input)
    pd.testing.assert_frame_equal(
            output.reset_index(drop=True), expected.reset_index(drop=True)
    )
class TestSplitPatientAndSampleData:
    def test_split_structure(self, test_input_clinical_data, test_cli_to_cbio_mapping):
        result = cli_to_cbio.split_into_patient_and_sample_data(test_input_clinical_data, test_cli_to_cbio_mapping)
        assert isinstance(result, dict)
        assert set(result.keys()) == {"merged", "patient", "sample"}


    def test_patient_columns(self, test_input_clinical_data, test_cli_to_cbio_mapping):
        result = cli_to_cbio.split_into_patient_and_sample_data(test_input_clinical_data, test_cli_to_cbio_mapping)
        patient_df = result["patient"]
        expected_cols = {"PATIENT_ID", "AGE", "Dataset"}
        assert expected_cols.issubset(set(patient_df.columns))


    def test_sample_columns(self, test_input_clinical_data, test_cli_to_cbio_mapping):
        result = cli_to_cbio.split_into_patient_and_sample_data(test_input_clinical_data, test_cli_to_cbio_mapping)
        sample_df = result["sample"]
        expected_cols = {
            "SAMPLE_ID", "PATIENT_ID", "CANCER_TYPE", "CANCER_TYPE_DETAILED",
            "STAGE", "TREATMENT", "Dataset"
        }
        assert expected_cols.issubset(set(sample_df.columns))


    def test_patient_deduplication(self, test_input_clinical_data, test_cli_to_cbio_mapping):
        # Add a duplicate row
        duplicated = pd.concat([test_input_clinical_data,test_input_clinical_data.iloc[[0]]], ignore_index=True)
        result = cli_to_cbio.split_into_patient_and_sample_data(duplicated, test_cli_to_cbio_mapping)
        patient_df = result["patient"]
        assert len(patient_df) == 2  # Should drop duplicate


    def test_empty_mapping(self):
        input_data = pd.DataFrame({
            "PATIENT_ID": ["P1"],
            "SAMPLE_ID": ["S1"],
            "CANCER_TYPE": ["TypeA"],
            "CANCER_TYPE_DETAILED": ["SubtypeA"],
            "Dataset":["SAGE-1"]
        })

        empty_mapping = pd.DataFrame(columns=["ATTRIBUTE_TYPE", "NORMALIZED_HEADER"])

        result = cli_to_cbio.split_into_patient_and_sample_data(input_data, empty_mapping)
        assert "AGE" not in result["patient"].columns
        assert result["sample"].shape[1] == 5  # basic required + EXTRA_COLS


def test_that_get_updated_cli_attributes_updates_correctly():
    # Define input mappings (like the Synapse mapping file)
    mapping_df = pd.DataFrame(
        {
            "NORMALIZED_HEADER": ["AGE", "YEAR"],
            "DESCRIPTION": ["Age of patient", "Year of birth"],
            "DATA_TYPE": ["FLOAT", "NUMBER"],
            "PRIORITY": [1, 1],
            "ATTRIBUTE_TYPE":["PATIENT", "PATIENT"],
            "DISPLAY_NAME": ["AGE", "YEAR"]
        }
    )

    # Create a mock existing cli_attr file
    existing_attr_df = pd.DataFrame(
        {
            "NORMALIZED_COLUMN_HEADER": ["SEX", "AGE"],
            "DESCRIPTIONS": ["Sex of patient", "Age of patient"],
            "DATATYPE": ["STRING", "NUMBER"],
            "PRIORITY": [None, None],
        }
    )

    with patch.object(
        pd, "read_csv", return_value=existing_attr_df
    ), patch.object(pd.DataFrame, "to_csv"):
        # Call the function
        updated_attr_df = cli_to_cbio.get_updated_cli_attributes(mapping_df, "tempdir")

        # Check output
        expected_df = pd.DataFrame(
            {
                "NORMALIZED_COLUMN_HEADER": ["SEX","AGE", "YEAR"],
                "DESCRIPTIONS": ["Sex of patient","Age of patient",  "Year of birth"],
                "DATATYPE": ["STRING", "FLOAT", "NUMBER"],
                "PRIORITY": [None, "1", "1"],
                "ATTRIBUTE_TYPE":[float('nan'), "PATIENT", "PATIENT"],
                "DISPLAY_NAME": [float('nan'), "AGE", "YEAR"]
            }
        )
        pd.testing.assert_frame_equal(
            updated_attr_df.reset_index(drop=True), expected_df.reset_index(drop=True)
        )


def test_that_create_case_lists_map_returns_expected_map():
    # Create a fake clinical file in TSV format
    rows = [
        {"CANCER_TYPE": "LUNG", "SAMPLE_ID": "S1"},
        {"CANCER_TYPE": "LUNG", "SAMPLE_ID": "S2"},
        {"CANCER_TYPE": "BREAST", "SAMPLE_ID": "S3"},
    ]

    with tempfile.NamedTemporaryFile(
        mode="w+", newline="", suffix=".txt", delete=False
    ) as tf:
        writer = csv.DictWriter(
            tf, fieldnames=["CANCER_TYPE", "SAMPLE_ID"], dialect="excel-tab"
        )
        writer.writeheader()
        writer.writerows(rows)
        tf.seek(0)

        cancer_map, samples = cli_to_cbio.create_case_lists_map(tf.name)

        assert cancer_map == {"LUNG": ["S1", "S2"], "BREAST": ["S3"]}
        assert samples == ["S1", "S2", "S3"]


def test_write_single_oncotree_case_list_writes_correctly():
    study_id = "my_study"
    cancer_type = "LUNG"
    sample_ids = ["S1", "S2"]

    with tempfile.TemporaryDirectory() as tmpdir:
        case_list_path = cli_to_cbio.write_single_oncotree_case_list(
            cancer_type, sample_ids, study_id, tmpdir
        )

        # Verify file was created
        assert os.path.exists(case_list_path)

        # Verify file content
        with open(case_list_path) as f:
            contents = f.read()
            assert "LUNG" in contents
            assert "S1" in contents
            assert "S2" in contents
            assert f"{study_id}_LUNG" in contents


def test_that_write_case_list_all_writes_correctly():
    clinical_samples = ["S1", "S2", "S3"]
    study_id = "test_study"

    with tempfile.TemporaryDirectory() as tmpdir:
        files = cli_to_cbio.write_case_list_all(clinical_samples, tmpdir, study_id)

        # We expect no return files, but a specific file to exist
        expected_file = os.path.join(tmpdir, "cases_all.txt")
        assert os.path.exists(expected_file)

        with open(expected_file) as f:
            contents = f.read()
            assert "test_study_all" in contents
            assert "\t".join(clinical_samples) in contents


def test_that_write_case_list_files_writes_correctly():
    clinical_file_map = {"LUNG": ["S1", "S2"], "BREAST": ["S3"]}
    study_id = "test_study"

    with tempfile.TemporaryDirectory() as tmpdir:
        file_paths = cli_to_cbio.write_case_list_files(clinical_file_map, tmpdir, study_id)

        assert len(file_paths) == 2
        for path in file_paths:
            assert os.path.exists(path)
            with open(path) as f:
                contents = f.read()
                assert study_id in contents


@pytest.mark.parametrize(
    "input_df, expected_os, expected_pfs",
    [
        (
            pd.DataFrame({
                "PATIENT_ID": ["P1", "P2"],
                "OS_STATUS": [0, 1],
                "PFS_STATUS": [1, 0]
            }),
            ["0:LIVING", "1:DECEASED"],
            ["1:DECEASED", "0:LIVING"]
        ),
        (
            pd.DataFrame({
                "OS_STATUS": [0, 2, None],
                "PFS_STATUS": [1, 1, None]
            }),
            ["0:LIVING", 2.0, float('nan')],
            ["1:DECEASED", "1:DECEASED", float('nan')]
        ),
        (
            pd.DataFrame({
                "OS_STATUS": [0, 1],
                "PFS_STATUS": [0, 1],
                "OTHER_COL": ["note1", "note2"]
            }),
            ["0:LIVING", "1:DECEASED"],
            ["0:LIVING", "1:DECEASED"]
        ),
    ],
    ids = ["all_values_mapped", "partially_unmapped", "no_remapping"]
)
def test_that_remap_column_values_returns_expected(input_df, expected_os, expected_pfs):
    result = cli_to_cbio.remap_column_values(input_df)
    np.testing.assert_equal(result["OS_STATUS"].tolist(), expected_os)
    np.testing.assert_equal(result["PFS_STATUS"].tolist(), expected_pfs)

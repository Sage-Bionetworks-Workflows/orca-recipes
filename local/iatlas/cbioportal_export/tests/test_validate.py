import logging
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import validate


@pytest.mark.parametrize(
    "input_samples, neo_samples, expect_error",
    [
        # IDs match -> no error
        (["S1", "S2"], ["S1", "S2"], False),
        # IDs mismatch -> error
        (["S1", "S2"], ["S1"], True),
        # IDs mismatch -> error
        (["S1"], ["S1", "S3"], True),
        # IDs match -> no error
        (["1", "2"], [1, 2], False),
    ],
    ids=["ids_match", "less_neoantigen_samples", "more_neoantigen_samples", "mismatch_dtypes"],
)
def test_that_merge_in_neoantigen_study_data_does_expected(
    input_samples, neo_samples, expect_error
):
    input_df = pd.DataFrame(
        {"Tumor_Sample_Barcode": input_samples, "foo": range(len(input_samples))}
    )

    with mock.patch.object(
        validate.syn, "get", return_value=SimpleNamespace(path="dummy.tsv")
    ), mock.patch.object(
        pd,
        "read_csv",
        return_value=pd.DataFrame(
            {"Sample_ID": neo_samples, "SNV": list(range(len(neo_samples)))}
        ),
    ):
        # Use a mock logger so we can assert .error calls directly
        mock_logger = mock.Mock()
        validate.validate_that_neoantigen_maf_ids_are_equal(
            input_df, neoantigen_data_synid="synZZZZ", logger=mock_logger
        )
        # Logger behavior
        if expect_error:
            mock_logger.error.assert_called_with(
                "The Tumor_Sample_Barcode values in the maf data do not match the Sample_ID values in the neoantigen data."
            )
        else:
            mock_logger.error.assert_not_called()


@pytest.mark.parametrize(
    "df, expect_error, error",
    [
        # Case 1: No missing columns
        (
            pd.DataFrame(
                {col: ["dummy"] for col in validate.REQUIRED_MAF_COLS},
            ),
            False,
            ""
        ),
        # Case 2: Has missing column
        (
            pd.DataFrame(
                {
                    col: ["dummy"]
                    for col in validate.REQUIRED_MAF_COLS
                    if col not in ["Annotation_Status"]
                },
            ),
            True,
            "Missing required columns in data_mutations.txt: ['Annotation_Status']",
        ),
    ],
    ids=["no_missing_cols", "has_missing_col"],
)
def test_validate_that_required_columns_are_present_expected_logging(
    df, expect_error, error, caplog
):
    with caplog.at_level(logging.ERROR):
        validate.validate_that_required_columns_are_present(
            df, 
            required_cols = validate.REQUIRED_MAF_COLS,
            dataset_file_name = "data_mutations.txt")

    if expect_error:
        assert len(caplog.records) == 1 and caplog.records[0].message == error
    else:
        assert len(caplog.records) == 0

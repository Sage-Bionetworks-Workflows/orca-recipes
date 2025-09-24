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
    ],
    ids=["ids_match", "less_neoantigen_samples", "more_neoantigen_samples"],
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

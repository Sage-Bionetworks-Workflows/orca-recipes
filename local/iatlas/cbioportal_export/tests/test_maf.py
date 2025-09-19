import logging
import os
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
import pandas as pd

import maf as maf_to_cbio


@pytest.fixture
def syn_mock():
    return mock.create_autospec(maf_to_cbio.syn)


def test_that_read_and_merge_maf_files_returns_expected_when_has_maf_files(syn_mock):
    # Mock getChildren to return fake .maf files
    syn_mock.getChildren.return_value = [
        {"name": "file1.maf", "id": "syn1"},
        {"name": "file2.maf", "id": "syn2"},
    ]

    syn_mock.get.side_effect = lambda x: mock.Mock(path=f"/fake/path/{x}.maf")

    with mock.patch.object(maf_to_cbio, "syn", syn_mock), mock.patch.object(
        maf_to_cbio.pd, "read_csv"
    ) as mock_read_csv:

        mock_read_csv.side_effect = [
            pd.DataFrame({"col": [1]}),
            pd.DataFrame({"col": [2]}),
        ]

        result = maf_to_cbio.read_and_merge_maf_files("synFolder123")
        assert result.equals(pd.DataFrame({"col": [1, 2]}))


def test_read_and_merge_maf_files_returns_none_when_no_maf_files(syn_mock):
    # Return only non-MAF files
    syn_mock.getChildren.return_value = [
        {"name": "file1.txt", "id": "syn1"},
        {"name": "notes.docx", "id": "syn2"},
    ]

    with mock.patch.object(maf_to_cbio, "syn", syn_mock):
        result = maf_to_cbio.read_and_merge_maf_files("synFolder123")
        assert result is None


@pytest.mark.parametrize(
    "input_size,max_rows,expected_chunks",
    [
        (10, 5, 2),
        (0, 10, 0),
        (299999, 300000, 1),
        (300001, 300000, 2),
    ],
    ids=[
        "multiple_chunks",
        "single_chunk",
        "one_row_below_max_rows",
        "one_row_above_max_rows",
    ],
)
def test_that_split_into_chunks_creates_expected_chunks(
    input_size, max_rows, expected_chunks
):
    df = pd.DataFrame(
        {"Chromosome": ["chr1"] * input_size, "Gene": ["TP53"] * input_size}
    )
    with TemporaryDirectory() as tmpdir:
        dataset_name = "TESTDATA"
        datahub_tools_path = tmpdir
        out_dir = os.path.join(tmpdir, "add-clinical-header", dataset_name)
        os.makedirs(out_dir, exist_ok=True)

        result = maf_to_cbio.split_into_chunks(
            dataset_name, df, datahub_tools_path, max_rows
        )
        assert result == expected_chunks
        for i in range(expected_chunks):
            assert os.path.exists(os.path.join(out_dir, f"data_mutations_{i + 1}.txt"))


def test_that_postprocessing_removes_chrM_variants():
    df = pd.DataFrame(
        {
            "Chromosome": ["chr1", "chrM", "chr2", "chrM"],
            "Gene": ["TP53", "MT-ND1", "BRCA1", "MT-CO1"],
        }
    )
    result = maf_to_cbio.postprocessing(df)
    assert all(result["Chromosome"] != "chrM")
    assert len(result) == 2


@pytest.mark.parametrize(
    "input, output, error",
    [
        # Case 1: Rows are unequal -> Error
        (
            pd.DataFrame({"Tumor_Sample_Barcode": [10, 20, 20]}),
            pd.DataFrame({"Tumor_Sample_Barcode": [10, 20]}),
            "Output rows 2 are not equal to input rows 3.",
        ),
        # Case 2: output has duplicates -> Error
        (
            pd.DataFrame({"Tumor_Sample_Barcode": [10, 10, 30]}),
            pd.DataFrame({"Tumor_Sample_Barcode": [10, 10, 30]}),
            "There are duplicates in the output.",
        ),
        # Case 3: tumor_sample_barcode vals in output not input -> Error
        (
            pd.DataFrame({"Tumor_Sample_Barcode": [10, 23, 30]}),
            pd.DataFrame({"Tumor_Sample_Barcode": [10, 20, 30]}),
            "The Tumor_Sample_Barcode values are not equal in the output compared to input.",
        ),
    ],
    ids=["unequal_rows", "dups", "tumor_sample_barcode_not_equal"],
)
def test_that_validate_export_files_does_expected_error_logging(
    input, output, error, caplog
):
    with caplog.at_level(logging.ERROR):
        maf_to_cbio.validate_export_files(input, output)

    assert len(caplog.records) == 1 and caplog.records[0].message == error


def test_that_validate_export_files_has_no_logging_when_valid(caplog):
    # Rows are equal, no duplicates, and tumor_sample_barcode matches -> No error
    input = pd.DataFrame(
        {"Tumor_Sample_Barcode": [10, 20, 30], "Chromosome": ["M", "2", "1"]}
    )
    output = pd.DataFrame(
        {"Tumor_Sample_Barcode": [10, 20, 30], "Chromosome": ["M", "2", "1"]}
    )
    with caplog.at_level(logging.ERROR):
        maf_to_cbio.validate_export_files(input, output)

    assert len(caplog.records) == 0


@pytest.mark.parametrize(
    "df, expect_error",
    [
        # Case 1: no NAs → expect no error
        (
            pd.DataFrame(
                {
                    "t_ref_count": [10, 20, 30],
                    "t_alt_count": [1, 2, 3],
                }
            ),
            False,
        ),
        # Case 2: has NAs in one column → expect error
        (
            pd.DataFrame(
                {
                    "t_ref_count": [10, None, 30],
                    "t_alt_count": [1, 2, 3],
                }
            ),
            True,
        ),
        # Case 3: has NAs in all → expect error
        (
            pd.DataFrame(
                {
                    "t_ref_count": [10, None, 30],
                    "t_alt_count": [1, 2, None],
                }
            ),
            True,
        ),
        # Case 4: doesn't have required columns for validation
        (
            pd.DataFrame({"chromosome": [1, 2, 3]}),
            False,
        ),
    ],
    ids=["no_nas", "has_nas_in_one", "has_nas_in_all", "no_req_cols"],
)
def test_validate_that_allele_freq_are_not_na_does_expected_logging(
    df, expect_error, caplog
):
    with caplog.at_level(logging.ERROR):
        maf_to_cbio.validate_that_allele_freq_are_not_na(df)

    if expect_error:
        assert (
            len(caplog.records) == 1
            and caplog.records[0].message
            == "There are NAs in the allele frequency columns: ['t_ref_count', 't_alt_count']"
        )
    else:
        assert len(caplog.records) == 0


@pytest.mark.parametrize(
    "df, expect_error, error",
    [
        # Case 1: No missing columns
        (
            pd.DataFrame(
                {col: ["dummy"] for col in maf_to_cbio.REQUIRED_MAF_COLS},
            ),
            False,
            ""
        ),
        # Case 2: Has missing column
        (
            pd.DataFrame(
                {
                    col: ["dummy"]
                    for col in maf_to_cbio.REQUIRED_MAF_COLS
                    if col not in ["Annotation_Status"]
                },
            ),
            True,
            "Missing required columns in maf: ['Annotation_Status']",
        ),
    ],
    ids=["no_missing_cols", "has_missing_col"],
)
def test_validate_that_required_columns_are_present_expected_logging(
    df, expect_error, error, caplog
):
    with caplog.at_level(logging.ERROR):
        maf_to_cbio.validate_that_required_columns_are_present(df)

    if expect_error:
        assert len(caplog.records) == 1 and caplog.records[0].message == error
    else:
        assert len(caplog.records) == 0

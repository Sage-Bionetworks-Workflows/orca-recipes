import os
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
import pandas as pd

import maf_to_cbioportal as maf_to_cbio


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

import pytest
import pandas as pd
import os
from tempfile import TemporaryDirectory

import maf_to_cbioportal as maf_to_cbio


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

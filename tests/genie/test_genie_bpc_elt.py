import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, Mock

from src.genie import genie_bpc_elt


def test_upload_clinical_tables_success():
    conn = MagicMock()
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = [{"name": "clinical.csv", "id": "syn1"}]

    with patch.object(
        pd, "read_csv", return_value=pd.DataFrame({"a": [1]})
    ), patch.object(genie_bpc_elt, "write_to_snowflake") as mock_write:
        genie_bpc_elt.upload_clinical_tables(conn, mock_syn, "syn1", overwrite=True)
        mock_write.assert_called_once()
        
        
def test_upload_clinical_tables_skips_if_dataframe_is_empty():
    conn = MagicMock()
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = [{"name": "clinical.csv", "id": "syn1"}]

    with patch.object(
        pd, "read_csv", return_value=pd.DataFrame()
    ), patch.object(genie_bpc_elt, "write_to_snowflake") as mock_write:
        genie_bpc_elt.upload_clinical_tables(conn, mock_syn, "syn1", overwrite=True)
        mock_write.assert_not_called()


def test_upload_cbioportal_tables_excludes_expected_files():
    conn = MagicMock()
    mock_syn = MagicMock()
    mock_syn.getChildren.return_value = [
        {"name": "meta_study.txt", "id": "syn111"},
        {"name": "data_mutations.txt", "id": "syn222"},
    ]

    with patch.object(
        pd, "read_csv", return_value=pd.DataFrame({"a": [1]})
    ), patch.object(genie_bpc_elt, "write_to_snowflake") as mock_write:
        genie_bpc_elt.upload_cbioportal_tables(
            conn, mock_syn, "cohortA", "syn123", overwrite=True
        )
        mock_write.assert_called_once()  # one excluded, one written


@pytest.mark.parametrize(
    "file_name, cohort, expected",
    [
        # basic "data_" + ".txt" cleanup
        ("data_clinical_patient.txt", "bpc", "clinical_patient"),

        # cohort prefix cleanup
        ("genie_BLADDER_data_clinical_sample.txt", "BLADDER", "clinical_sample"),
        ("genie_19_3_data_clinical_patient.txt", "19_3", "clinical_patient"),

        # cna normalization rule
        ("genie_bpc_cna_hg19.seg", "bpc", "seg"),
        ("genie_bpc_data_cna_hg19.seg", "bpc", "seg"),

        # no-op-ish cases
        # If no matching patterns, string comes back mostly unchanged
        ("meta_clinical.txt", "bpc", "meta_clinical"),
        ("gene_panel.txt", "bpc", "gene_panel"),
        ("random_file_name", "bpc", "random_file_name"),

        # different cohort should not remove wrong prefix
        ("genie_bpc_data_clinical_patient.txt", "public", "genie_bpc_clinical_patient"),
    ],
)
def test_get_table_name(file_name, cohort, expected):
    assert genie_bpc_elt.get_table_name(file_name, cohort) == expected
    
    
@pytest.mark.parametrize(
    "cohort, version, database, expected_schema",
    [
        ("bpc", "1_0", "GENIE_DB", "bpc_1_0"),
        ("public", "19_3", "FAKE_DB", "public_19_3"),
        ("19_3", "consortium", "GENIE", "19_3_consortium"),
    ],
)
def test_create_snowflake_resources_runs_sql_and_calls_uploads(
    cohort, version, database, expected_schema
):
    mock_conn = MagicMock(name="conn")
    mock_syn = MagicMock(name="syn")
    mock_cursor = MagicMock(name="cursor")

    # conn.cursor() is used as a context manager
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None

    with patch.object(genie_bpc_elt, "upload_clinical_tables") as mock_upload_clinical, \
         patch.object(genie_bpc_elt, "upload_cbioportal_tables") as mock_upload_cbio, \
         patch.object(genie_bpc_elt, "logger") as mock_logger:

        genie_bpc_elt.create_snowflake_resources(
            conn=mock_conn,
            syn=mock_syn,
            cohort=cohort,
            version=version,
            clinical_synid="syn_clin",
            cbioportal_synid="syn_cbio",
            overwrite=True,
            database=database,
        )

    # sql executed with expected schema/database
    mock_cursor.execute.assert_any_call(f"USE DATABASE {database};")
    mock_cursor.execute.assert_any_call(
        f"CREATE SCHEMA IF NOT EXISTS {expected_schema} WITH MANAGED ACCESS;"
    )
    mock_cursor.execute.assert_any_call(f"USE SCHEMA {expected_schema}")

    # upload helpers called once with correct args
    mock_upload_clinical.assert_called_once_with(
        conn=mock_conn,
        syn=mock_syn,
        clinical_synid="syn_clin",
        overwrite=True,
    )

    mock_upload_cbio.assert_called_once_with(
        conn=mock_conn,
        syn=mock_syn,
        cohort=cohort,
        cbioportal_synid="syn_cbio",
        overwrite=True,
    )

    mock_logger.info.assert_any_call(f"Creating/using schema: {expected_schema}")

    # check that cursor cm entered/exited
    mock_conn.cursor.assert_called_once()
    mock_conn.cursor.return_value.__enter__.assert_called_once()
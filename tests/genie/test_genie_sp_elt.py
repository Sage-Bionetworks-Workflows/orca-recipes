import pytest
from unittest.mock import MagicMock, patch
import pandas as pd

from genie import genie_sp_elt


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = MagicMock()
    return conn


@pytest.fixture
def mock_syn():
    syn = MagicMock()
    syn.getChildren.return_value = [
        {"id": "file1"},
        {"id": "file2"}
    ]
    file_entity = MagicMock()
    file_entity.path = "/tmp/file.tsv"
    syn.get.return_value = file_entity
    return syn


@pytest.fixture
def cohort_config_single():
    return {
        "cohort": "test_cohort",
        "table_name": "test_table",
        "folder_synid": "syn123"
    }


@pytest.fixture
def cohort_config_list():
    return {
        "cohort": "test_cohort_list",
        "table_name": "test_table",
        "folder_synid": ["syn1", "syn2"]
    }


def test_process_cohort_single(mock_syn, mock_conn, cohort_config_single):
    with patch.object(genie_sp_elt, "pandas") as mock_pd, \
         patch.object(genie_sp_elt, "write_to_snowflake") as mock_write:

        # Mock pandas.read_csv to return a dataframe
        mock_pd.read_csv.return_value = pd.DataFrame({"a": [1, 2]})

        genie_sp_elt.process_cohort(
            syn=mock_syn,
            conn=mock_conn,
            cohort_config=cohort_config_single,
            database="TEST_DB",
            overwrite=True
        )

        assert mock_pd.read_csv.call_count == 2
        assert mock_write.call_count == 2
        mock_conn.cursor.return_value.__enter__.return_value.execute.assert_any_call("USE DATABASE TEST_DB;")
        mock_conn.cursor.return_value.__enter__.return_value.execute.assert_any_call(
            "CREATE SCHEMA IF NOT EXISTS test_cohort WITH MANAGED ACCESS;"
        )


def test_process_cohort_folder_list(mock_syn, mock_conn, cohort_config_list):
    with patch.object(genie_sp_elt, "pandas") as mock_pd, \
         patch.object(genie_sp_elt, "write_to_snowflake") as mock_write:

        mock_pd.read_csv.return_value = pd.DataFrame({"a": [1]})

        genie_sp_elt.process_cohort(
            syn=mock_syn,
            conn=mock_conn,
            cohort_config=cohort_config_list,
            database="TEST_DB",
            overwrite=False
        )

        # 2 folders × 2 files each
        assert mock_pd.read_csv.call_count == 4
        assert mock_write.call_count == 4


def test_process_cohort_empty_file(mock_syn, mock_conn, cohort_config_single):
    with patch.object(genie_sp_elt, "pandas") as mock_pd, \
         patch.object(genie_sp_elt, "write_to_snowflake") as mock_write:

        mock_pd.read_csv.side_effect = pd.errors.EmptyDataError

        genie_sp_elt.process_cohort(
            syn=mock_syn,
            conn=mock_conn,
            cohort_config=cohort_config_single,
            database="TEST_DB",
            overwrite=True
        )

        mock_write.assert_not_called()


def test_process_cohort_read_exception(mock_syn, mock_conn, cohort_config_single):
    with patch.object(genie_sp_elt, "pandas") as mock_pd, \
         patch.object(genie_sp_elt, "write_to_snowflake") as mock_write:

        mock_pd.read_csv.side_effect = Exception("Failed read")

        genie_sp_elt.process_cohort(
            syn=mock_syn,
            conn=mock_conn,
            cohort_config=cohort_config_single,
            database="TEST_DB",
            overwrite=True
        )

        mock_write.assert_not_called()

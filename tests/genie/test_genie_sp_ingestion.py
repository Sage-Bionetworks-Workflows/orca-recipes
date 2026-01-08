import pytest
from unittest.mock import MagicMock, patch, call
import pandas as pd

from src.genie import genie_sp_ingestion as ingest


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = MagicMock()
    return conn


@pytest.fixture
def mock_syn():
    syn = MagicMock()
    syn.getChildren.return_value = [
        {"id": "file1", "name": "f1.tsv"},
        {"id": "file2", "name": "f2.tsv"},
    ]
    file_entity = MagicMock()
    file_entity.path = "/tmp/file.tsv"
    syn.get.return_value = file_entity
    return syn


@pytest.fixture
def cohort_config_single():
    return {"cohort": "test_cohort", "table_name": "test_table", "file_synid": "syn123"}


@pytest.fixture
def cohort_config_list():
    return {
        "cohort": "test_cohort_list",
        "table_name": "test_table",
        "file_synid": ["syn1", "syn2"],
    }


def test_process_cohort_single(mock_syn, mock_conn, cohort_config_single):
    """
    Single file_synid (string) should be coerced to list and overwrite forced False.
    Also validates Snowflake schema setup SQL and 2 files ingested.
    """
    cursor = mock_conn.cursor.return_value.__enter__.return_value

    with patch.object(ingest, "pd") as mock_pd, patch.object(
        ingest, "write_to_snowflake"
    ) as mock_write:

        df = pd.DataFrame({"a": [1, 2]})
        mock_pd.read_csv.return_value = df

        ingest.process_cohort(
            syn=mock_syn,
            conn=mock_conn,
            cohort_config=cohort_config_single,
            database="TEST_DB",
            overwrite=True,  # should be ignored / forced False internally
        )

        # 1 files
        assert mock_pd.read_csv.call_count == 1
        assert mock_write.call_count == 1

        # overwrite will end up always False for single file case
        for c in mock_write.call_args_list:
            assert c.kwargs["overwrite"] is True
            assert c.kwargs["table_name"] == "test_table"
            assert c.kwargs["conn"] == mock_conn
            assert c.kwargs["table_df"] is df

        # SQL setup executed in order
        cursor.execute.assert_has_calls(
            [
                call("USE DATABASE TEST_DB;"),
                call("CREATE SCHEMA IF NOT EXISTS test_cohort WITH MANAGED ACCESS;"),
                call("USE SCHEMA test_cohort"),
            ],
            any_order=False,
        )


def test_process_cohort_file_list(mock_syn, mock_conn, cohort_config_list):
    """
    List file_synid keeps overwrite as provided.
    Expect 4 reads/writes.
    """
    with patch.object(ingest, "pd") as mock_pd, patch.object(
        ingest, "write_to_snowflake"
    ) as mock_write:

        df = pd.DataFrame({"a": [1]})
        mock_pd.read_csv.return_value = df

        ingest.process_cohort(
            syn=mock_syn,
            conn=mock_conn,
            cohort_config=cohort_config_list,
            database="TEST_DB",
            overwrite=False,
        )

        # 2 files
        assert mock_pd.read_csv.call_count == 2
        assert mock_write.call_count == 2

        # overwrite should remain False for list case
        for c in mock_write.call_args_list:
            assert c.kwargs["overwrite"] is False
            assert c.kwargs["table_name"] == "test_table"
            assert c.kwargs["conn"] == mock_conn
            assert c.kwargs["table_df"] is df


def test_process_cohort_skips_empty_files(mock_syn, mock_conn, cohort_config_single):
    """
    Empty dataframe should skip write_to_snowflake.
    Still reads both files.
    """
    with patch.object(ingest, "pd") as mock_pd, patch.object(
        ingest, "write_to_snowflake"
    ) as mock_write:

        mock_pd.read_csv.return_value = pd.DataFrame()  # empty => df.empty True

        ingest.process_cohort(
            syn=mock_syn,
            conn=mock_conn,
            cohort_config=cohort_config_single,
            database="TEST_DB",
            overwrite=True,
        )

        assert mock_pd.read_csv.call_count == 1
        mock_write.assert_not_called()

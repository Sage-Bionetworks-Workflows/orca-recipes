import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from snowflake import utils


def test_table_exists_true():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("table",)
    mock_conn = MagicMock(schema="s", database="d")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    result = utils.table_exists(mock_conn, "tbl")
    assert result is True


def test_table_exists_false():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn = MagicMock(schema="s", database="d")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    result = utils.table_exists(mock_conn, "tbl")
    assert result is False


def test_write_to_snowflake_success(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    conn = MagicMock()

    with patch.object(utils, "table_exists", return_value=False), \
         patch.object(utils, "write_pandas", return_value=(True, 1, 1, None)) as mock_wp:
        utils.write_to_snowflake(conn, df, "t1", overwrite=True)
        mock_wp.assert_called_once()


def test_write_to_snowflake_skip(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    conn = MagicMock()

    with patch.object(utils, "table_exists", return_value=True), \
         patch.object(utils, "write_pandas", return_value=(True, 1, 1, None)) as mock_wp:
        utils.write_to_snowflake(conn, df, "t1", overwrite=False)
        mock_wp.assert_not_called()


def test_write_to_snowflake_invalid_kwargs():
    df = pd.DataFrame({"a": [1]})
    conn = MagicMock()

    with patch.object(utils, "table_exists", return_value=False):
        with pytest.raises(ValueError):
            utils.write_to_snowflake(conn, df, "t1", write_pandas_kwargs={"bad_arg": 1})

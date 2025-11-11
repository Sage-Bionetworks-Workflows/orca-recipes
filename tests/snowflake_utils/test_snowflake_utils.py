from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from snowflake_utils import get_connection, table_exists, write_to_snowflake


class TestConnector:
    def test_get_connection_success(self, monkeypatch):
        env_vars = {
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pw",
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_DATABASE": "db",
            "SNOWFLAKE_SCHEMA": "schema",
            "SNOWFLAKE_ROLE": "SYSADMIN",
            "SNOWFLAKE_WAREHOUSE": "compute_xsmall",
        }
        for k, v in env_vars.items():
            monkeypatch.setenv(k, v)

        mock_conn = MagicMock()

        # patch snowflake connector inside snowflake_utils namespace
        with patch(
            "snowflake.connector.connect", return_value=mock_conn
        ) as mock_connect:
            conn = get_connection()
            assert conn == mock_conn
            mock_connect.assert_called_once()

    def test_get_connection_missing_env(self, monkeypatch):
        # Ensure at least one required env var is missing
        monkeypatch.delenv("SNOWFLAKE_USER", raising=False)

        with pytest.raises(EnvironmentError):
            get_connection()

    def test_get_connection_fails(self, monkeypatch):
        # Set required env vars
        vars = [
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_WAREHOUSE",
        ]
        for v in vars:
            monkeypatch.setenv(v, "x")

        # Patch the correct namespace and simulate failure
        with patch("snowflake.connector.connect", side_effect=Exception("bad creds")):
            with pytest.raises(Exception, match="bad creds"):
                get_connection()


class TestUtils:
    def test_table_exists_true(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("table",)
        mock_conn = MagicMock(schema="s", database="d")
        mock_conn.cursor.return_value = mock_cursor

        assert table_exists(mock_conn, "tbl") is True

    def test_table_exists_false(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock(schema="s", database="d")
        mock_conn.cursor.return_value = mock_cursor

        assert table_exists(mock_conn, "tbl") is False

    def test_write_to_snowflake_success(self):
        df = pd.DataFrame({"a": [1]})
        conn = MagicMock()

        with patch("snowflake_utils.table_exists", return_value=False), patch(
            "snowflake_utils.snowflake_utils.write_pandas",
            return_value=(True, 1, 1, None),
        ) as mock_wp:
            write_to_snowflake(conn, df, "t1", overwrite=True)
            mock_wp.assert_called_once()

    def test_write_to_snowflake_skip(self):
        df = pd.DataFrame({"a": [1]})
        conn = MagicMock()

        with patch("snowflake_utils.table_exists", return_value=True), patch(
            "snowflake_utils.snowflake_utils.write_pandas",
            return_value=(True, 1, 1, None),
        ) as mock_wp:
            write_to_snowflake(conn, df, "t1", overwrite=False)
            mock_wp.assert_not_called()

    def test_write_to_snowflake_invalid_kwargs(self):
        df = pd.DataFrame({"a": [1]})
        conn = MagicMock()

        with patch("snowflake_utils.table_exists", return_value=False):
            with pytest.raises(ValueError):
                write_to_snowflake(conn, df, "t1", write_pandas_kwargs={"bad_arg": 1})

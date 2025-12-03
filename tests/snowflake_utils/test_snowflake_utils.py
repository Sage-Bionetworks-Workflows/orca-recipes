from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.snowflake_utils import get_cursor, get_connection, table_exists, write_to_snowflake


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


class TestCursor:
    def _make_conn_and_cursor(self):
        """
        Helper to build a fake Snowflake connection whose cursor() is
        a context manager yielding a cursor object.
        """
        conn = MagicMock(name="conn")
        cursor_cm = MagicMock(name="cursor_cm")
        cursor = MagicMock(name="cursor")

        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = None

        conn.cursor.return_value = cursor_cm
        return conn, cursor_cm, cursor

    def test_get_cursor_with_passed_conn_does_not_create_or_close_local_conn(self):
        """
        Test that when passed a Snowflake connection object, it will not create or close local
        connection since there is none.
        """
        conn, cursor_cm, cursor = self._make_conn_and_cursor()

        # patch create_local_connection so we can assert it's NOT called
        with patch(
            "snowflake_utils.snowflake_utils.create_local_connection"
        ) as mock_create_local:
            with get_cursor(conn=conn) as cs:
                # yielded cursor should be the __enter__ result
                assert cs is cursor

            mock_create_local.assert_not_called()

        # Make sure cursor() was used as a context manager
        conn.cursor.assert_called_once()
        cursor_cm.__enter__.assert_called_once()
        cursor_cm.__exit__.assert_called_once()

        # When conn is passed, we should not close it
        conn.close.assert_not_called()

    def test_get_cursor_without_conn_creates_and_closes_local_conn(self):
        """
        Test that when not passed a snowflake connection object (e.g from Airflow's SnowHook),
        a local connection is created and eventually closed
        """
        local_conn, cursor_cm, cursor = self._make_conn_and_cursor()

        with patch, object(
            "snowflake_utils.snowflake_utils.create_local_connection",
            return_value=local_conn,
        ) as mock_create_local:
            with get_cursor(conn=None) as cs:
                assert cs is cursor

            mock_create_local.assert_called_once()

        # Cursor context manager used
        local_conn.cursor.assert_called_once()
        cursor_cm.__enter__.assert_called_once()
        cursor_cm.__exit__.assert_called_once()

        # Because local_conn was created inside, it must be closed in finally
        local_conn.close.assert_called_once()

    def test_get_cursor_closes_local_conn_even_on_exception(self):
        """
        Test that when using local credentials to connect to Snowflake,
        a local connection is created and eventually closed
        """
        local_conn, cursor_cm, cursor = self._make_conn_and_cursor()

        with patch(
            "snowflake_utils.snowflake_utils.create_local_connection",
            return_value=local_conn,
        ):
            with pytest.raises(RuntimeError):
                with get_cursor() as cs:
                    assert cs is cursor
                    raise RuntimeError("boom")

        # still closes connection
        local_conn.close.assert_called_once()
        # and still exited cursor CM
        cursor_cm.__exit__.assert_called_once()


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

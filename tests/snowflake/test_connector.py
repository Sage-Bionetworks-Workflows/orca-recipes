import pytest
from unittest.mock import patch, MagicMock
from snowflake import connector


def test_get_connection_success(monkeypatch):
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
    with patch.object(connector, "snowflake", autospec=True) as mock_sf:
        mock_sf.connector.connect.return_value = mock_conn
        conn = connector.get_connection()
        assert conn == mock_conn
        mock_sf.connector.connect.assert_called_once()


def test_get_connection_missing_env(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    with pytest.raises(EnvironmentError):
        connector.get_connection()


def test_get_connection_fails(monkeypatch):
    for v in [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]:
        monkeypatch.setenv(v, "x")

    with patch.object(connector, "snowflake", autospec=True) as mock_sf:
        mock_sf.connector.connect.side_effect = Exception("bad creds")
        with pytest.raises(Exception, match="bad creds"):
            connector.get_connection()

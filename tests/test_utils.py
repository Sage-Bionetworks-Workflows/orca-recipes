from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowNotFoundException

from src.utils import validate_required_secrets


class TestValidateRequiredSecrets:
    """Tests for validate_required_secrets."""

    @patch("src.utils.Variable")
    @patch("src.utils.BaseHook")
    def test_all_resolved(self, mock_base_hook: MagicMock, mock_variable: MagicMock) -> None:
        """No error is raised when every connection and variable resolves."""
        mock_base_hook.get_connection.return_value = MagicMock()
        mock_variable.get.return_value = "some_value"

        # When validating a connection and a variable that both resolve
        validate_required_secrets(["conn_a"], ["var_a"])

        # Then each one is looked up exactly once
        mock_base_hook.get_connection.assert_called_once_with("conn_a")
        mock_variable.get.assert_called_once_with("var_a")

    @patch("src.utils.Variable")
    @patch("src.utils.BaseHook")
    def test_multiple_all_resolved(self, mock_base_hook: MagicMock, mock_variable: MagicMock) -> None:
        """Every connection ID and variable name provided is checked."""
        mock_base_hook.get_connection.return_value = MagicMock()
        mock_variable.get.return_value = "some_value"

        # When validating multiple connections and variables that all resolve
        validate_required_secrets(["conn_a", "conn_b"], ["var_a", "var_b"])

        # Then every one of them is looked up
        assert mock_base_hook.get_connection.call_count == 2
        assert mock_variable.get.call_count == 2

    @patch("src.utils.Variable")
    @patch("src.utils.BaseHook")
    def test_missing_connection(self, mock_base_hook: MagicMock, mock_variable: MagicMock) -> None:
        """An unresolvable connection raises a ValueError naming it."""
        mock_base_hook.get_connection.side_effect = AirflowNotFoundException()
        mock_variable.get.return_value = "some_value"

        # When the connection can't be resolved
        # Then a ValueError naming that connection is raised
        with pytest.raises(ValueError, match="connection: conn_a"):
            validate_required_secrets(["conn_a"], ["var_a"])

    @patch("src.utils.Variable")
    @patch("src.utils.BaseHook")
    def test_missing_variable(self, mock_base_hook: MagicMock, mock_variable: MagicMock) -> None:
        """An unresolvable variable raises a ValueError naming it."""
        mock_base_hook.get_connection.return_value = MagicMock()
        mock_variable.get.side_effect = KeyError("var_a")

        # When the variable can't be resolved
        # Then a ValueError naming that variable is raised
        with pytest.raises(ValueError, match="variable: var_a"):
            validate_required_secrets(["conn_a"], ["var_a"])

    @patch("src.utils.Variable")
    @patch("src.utils.BaseHook")
    def test_missing_both(self, mock_base_hook: MagicMock, mock_variable: MagicMock) -> None:
        """Both a missing connection and a missing variable are reported together."""
        mock_base_hook.get_connection.side_effect = AirflowNotFoundException()
        mock_variable.get.side_effect = KeyError("var_a")

        # When neither the connection nor the variable can be resolved
        with pytest.raises(ValueError) as exc_info:
            validate_required_secrets(["conn_a"], ["var_a"])

        # Then the error message names both of them
        assert "connection: conn_a" in str(exc_info.value)
        assert "variable: var_a" in str(exc_info.value)

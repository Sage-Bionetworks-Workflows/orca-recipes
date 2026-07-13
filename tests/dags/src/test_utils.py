"""Tests for dags/src/utils.py."""

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowNotFoundException

from dags.src.utils import validate_required_secrets


class TestValidateRequiredSecrets:
    """Tests for validate_required_secrets."""

    @patch("dags.src.utils.Variable")
    @patch("dags.src.utils.BaseHook")
    def test_all_resolved(self, mock_base_hook: MagicMock, mock_variable: MagicMock) -> None:
        """No error is raised when every connection and variable resolves."""
        mock_base_hook.get_connection.return_value = MagicMock()
        mock_variable.get.return_value = "some_value"

        # When validating a connection and a variable that both resolve
        validate_required_secrets(["conn_a"], ["var_a"])

        # Then each one is looked up exactly once
        mock_base_hook.get_connection.assert_called_once_with("conn_a")
        mock_variable.get.assert_called_once_with("var_a")

    @patch("dags.src.utils.Variable")
    @patch("dags.src.utils.BaseHook")
    def test_multiple_all_resolved(self, mock_base_hook: MagicMock, mock_variable: MagicMock) -> None:
        """Every connection ID and variable name provided is checked."""
        mock_base_hook.get_connection.return_value = MagicMock()
        mock_variable.get.return_value = "some_value"

        # When validating multiple connections and variables that all resolve
        validate_required_secrets(["conn_a", "conn_b"], ["var_a", "var_b"])

        # Then every one of them is looked up
        assert mock_base_hook.get_connection.call_count == 2
        assert mock_variable.get.call_count == 2

    @pytest.mark.parametrize(
        "connection_fails, variable_fails, expected_substrings",
        [
            (True, False, ["connection: conn_a"]),
            (False, True, ["variable: var_a"]),
            (True, True, ["connection: conn_a", "variable: var_a"]),
        ],
        ids=["missing_connection", "missing_variable", "missing_both"],
    )
    @patch("dags.src.utils.Variable")
    @patch("dags.src.utils.BaseHook")
    def test_missing_secrets_raise_value_error(
        self,
        mock_base_hook: MagicMock,
        mock_variable: MagicMock,
        connection_fails: bool,
        variable_fails: bool,
        expected_substrings: list[str],
    ) -> None:
        """Unresolvable connections/variables are named in the raised ValueError."""
        mock_base_hook.get_connection.side_effect = (
            AirflowNotFoundException() if connection_fails else None
        )
        mock_variable.get.side_effect = KeyError("var_a") if variable_fails else None
        mock_variable.get.return_value = "some_value"

        # When the connection and/or variable can't be resolved
        with pytest.raises(ValueError) as exc_info:
            validate_required_secrets(["conn_a"], ["var_a"])

        # Then the error message names each unresolvable one
        for substring in expected_substrings:
            assert substring in str(exc_info.value)

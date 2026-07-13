# tests/src/test_utils.py

import logging

from airflow.exceptions import AirflowNotFoundException
import pytest
from unittest.mock import MagicMock, patch

from dags.src.utils import DEFAULT_LOGGER_PREFIX, get_logger, validate_required_secrets


@pytest.mark.parametrize("name", [None, ""], ids = ["None", "Empty string"])
def test_get_logger_without_name_returns_default_logger(name):
    logger = get_logger(name)

    assert isinstance(logger, logging.Logger)
    assert logger.name == DEFAULT_LOGGER_PREFIX


def test_get_logger_prefixes_module_name():
    logger = get_logger("src.synapse_alerts")

    assert logger.name == "sage_airflow.src.synapse_alerts"


def test_get_logger_does_not_duplicate_existing_prefix():
    logger = get_logger("sage_airflow.src.synapse_alerts")

    assert logger.name == "sage_airflow.src.synapse_alerts"


def test_get_logger_returns_same_logger_for_same_name():
    first_logger = get_logger("src.synapse_alerts")
    second_logger = get_logger("src.synapse_alerts")

    assert first_logger is second_logger


def test_get_logger_returns_different_loggers_for_different_modules():
    alerts_logger = get_logger("src.synapse_alerts")
    hook_logger = get_logger("src.synapse_hook")

    assert alerts_logger is not hook_logger
    assert alerts_logger.name == "sage_airflow.src.synapse_alerts"
    assert hook_logger.name == "sage_airflow.src.synapse_hook"


def test_get_logger_does_not_add_handlers():
    logger = get_logger("src.synapse_alerts")

    assert logger.handlers == []


def test_logger_propagates_to_parent_logging_configuration():
    logger = get_logger("src.synapse_alerts")

    assert logger.propagate is True
"""Tests for dags/src/utils.py."""


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

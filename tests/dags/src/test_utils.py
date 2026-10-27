"""Tests for dags/src/utils.py."""

import logging

from airflow.exceptions import AirflowNotFoundException
from botocore.exceptions import ClientError, NoCredentialsError, TokenRetrievalError
import pytest
from unittest.mock import MagicMock, patch

from dags.src.utils import (
    DEFAULT_LOGGER_PREFIX,
    get_logger,
    validate_aws_credentials,
    validate_required_secrets,
)


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
        mock_variable.get.assert_called_once_with("var_a", default=None)

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
        mock_variable.get.return_value = None if variable_fails else "some_value"

        # When the connection and/or variable can't be resolved
        with pytest.raises(ValueError) as exc_info:
            validate_required_secrets(["conn_a"], ["var_a"])

        # Then the error message names each unresolvable one
        for substring in expected_substrings:
            assert substring in str(exc_info.value)


class TestValidateAwsCredentials:
    """Tests for validate_aws_credentials."""

    ARN = "arn:aws:sts::123456789012:assumed-role/Developer/someone"

    @patch("boto3.client")
    def test_valid_credentials_return_the_caller_arn(self, mock_client: MagicMock) -> None:
        """The ARN of the caller is returned when STS accepts the credentials."""
        mock_client.return_value.get_caller_identity.return_value = {"Arn": self.ARN}

        # When the credentials are valid
        arn = validate_aws_credentials()

        # Then the identity is returned, from an STS client in the default region
        assert arn == self.ARN
        mock_client.assert_called_once_with("sts", region_name="us-east-1")

    @patch("boto3.client")
    def test_region_name_is_passed_to_the_client(self, mock_client: MagicMock) -> None:
        """The check doesn't depend on a region being configured locally."""
        mock_client.return_value.get_caller_identity.return_value = {"Arn": self.ARN}

        validate_aws_credentials(region_name="eu-west-1")

        mock_client.assert_called_once_with("sts", region_name="eu-west-1")

    @pytest.mark.parametrize(
        "error",
        [
            NoCredentialsError(),
            TokenRetrievalError(provider="sso", error_msg="The SSO session has expired"),
            ClientError(
                {"Error": {"Code": "ExpiredToken", "Message": "The token is expired"}},
                "GetCallerIdentity",
            ),
        ],
        ids=["no_credentials", "expired_sso_session", "expired_token"],
    )
    @patch("boto3.client")
    def test_unusable_credentials_raise_value_error(
        self, mock_client: MagicMock, error: Exception
    ) -> None:
        """Missing or expired credentials give a ValueError that says how to log in."""
        mock_client.return_value.get_caller_identity.side_effect = error

        # When AWS rejects, or can't find, the credentials
        with pytest.raises(ValueError) as exc_info:
            validate_aws_credentials()

        # Then the message names both login steps and keeps the original error
        message = str(exc_info.value)
        assert "aws sso login" in message
        assert "AWS_PROFILE" in message
        assert str(error) in message
        assert exc_info.value.__cause__ is error

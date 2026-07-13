# tests/src/test_utils.py

import logging

import pytest

from src.utils import DEFAULT_LOGGER_PREFIX, get_logger


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

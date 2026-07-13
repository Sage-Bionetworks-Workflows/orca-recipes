"""Utility functions for various components of the Airflow DAGs."""
import logging
from typing import Optional


DEFAULT_LOGGER_PREFIX = "sage_airflow"


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger that integrates with Airflow's logging configuration.

       Logger names are prefixed with "sage_airflow" to avoid
        conflicts with other loggers.

    Args:
        name: Module or component name. Passing __name__ is recommended

    Returns:
        A configured logger managed by Airflow's root logging configuration
    """
    if not name:
        return logging.getLogger(DEFAULT_LOGGER_PREFIX)

    if name.startswith(f"{DEFAULT_LOGGER_PREFIX}."):
        logger_name = name
    else:
        logger_name = f"{DEFAULT_LOGGER_PREFIX}.{name}"

    return logging.getLogger(logger_name)

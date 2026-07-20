"""Utility functions for various components of the Airflow DAGs."""
import logging
from typing import Optional

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Variable


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


def validate_required_secrets(connection_ids: list[str], variable_names: list[str]) -> None:
    """Fail fast if any Connection/Variable this DAG needs isn't resolvable.

    Args:
        connection_ids: Airflow connection IDs that must resolve via BaseHook.get_connection.
        variable_names: Airflow Variable names that must resolve via Variable.get.

    Raises:
        ValueError: If any connection or variable fails to resolve, listing each
            missing one as "connection: <id>" or "variable: <name>", e.g.:
            "Missing required secrets before running locally:\\n  connection: MY_CONN\\n  variable: MY_VAR"
    """
    missing = []

    for conn_id in connection_ids:
        try:
            BaseHook.get_connection(conn_id)
        except AirflowNotFoundException:
            missing.append(f"connection: {conn_id}")

    for var_name in variable_names:
        try:
            Variable.get(var_name)
        except KeyError:
            missing.append(f"variable: {var_name}")

    if missing:
        raise ValueError(
            "Missing required secrets before running locally:\n  "
            + "\n  ".join(missing)
        )

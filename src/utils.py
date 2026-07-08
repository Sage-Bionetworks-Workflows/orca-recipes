from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Variable

def validate_required_secrets(connection_ids: list[str], variable_names: list[str]) -> None:
    """Fail fast if any Connection/Variable this DAG needs isn't resolvable.

    Args:
        connection_ids: Airflow connection IDs that must resolve via BaseHook.get_connection.
        variable_names: Airflow Variable names that must resolve via Variable.get.
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
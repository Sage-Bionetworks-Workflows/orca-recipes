"""Shared helpers for sending Synapse message alerts from any DAG.

Synapse delivers messages as email, so this is a dependency-free way for a DAG to
alert people (no SMTP setup required). The main entry points are:

- send_synapse_message - sends a custom message via email to Synapse users.
- synapse_failure_callback - build on_failure_callback for a DAG that emails a
  standard failure alert to the DAG's configured recipients, with an optional
  DAG specific note

See CONTRIBUTION.md section DAG Failure Alerts for more info and usage examples.
"""
import logging
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from src.synapse_hook import SynapseHook

logger = logging.getLogger(__name__)


def is_shareable_url(url: str) -> bool:
    """Check if a URL is shareable (not localhost or private IP).

    Args:
        url (str): The URL to check

    Returns:
        bool: True if the URL is shareable, False otherwise
    """
    hostname = urlparse(url).hostname
    return hostname not in {"localhost", "127.0.0.1", "0.0.0.0", None}


def send_synapse_message(
    conn_id: str, usernames: List[str], subject: str, body: str
) -> None:
    """Send a Synapse message (delivered as email) to a list of users.

    Arguments:
        conn_id (str): Synapse connection id
        usernames (List[str]): Synapse usernames or numeric owner ids to message
        subject (str): Message subject
        body (str): Message body
    """
    usernames = [u.strip() for u in usernames if u.strip()]
    if not usernames:
        logger.warning(f"No Synapse users provided; skipping message: {subject}")
        return
    client = SynapseHook(conn_id).client
    owner_ids = [client.getUserProfile(u).get("ownerId") for u in usernames]
    client.sendMessage(owner_ids, subject, body)
    logger.info(f"Sent Synapse message '{subject}' to {usernames}")


def synapse_failure_callback(
    message: Optional[str] = None,
    conn_id_param: str = "synapse_conn_id",
    user_list_param: str = "dev_user_list",
) -> Callable[[Dict[str, Any]], None]:
    """Build a on_failure_callback for a DAG that emails a Synapse failure alert.

    The returned callback reads the Synapse connection id and the recipient list
    from the DAG's params (so each DAG controls who gets alerted), builds a
    standard failure message (DAG id, task id, run id, execution date, exception,
    and a link to the task logs), and optionally appends a DAG-specific note.

    The send is wrapped in a try/except so a notification failure can never mask
    the original task error.
    
    If the task logs are not available as a shareable URL (e.g. when running Airflow 
    locally or in GitHub Codespaces), the alert will include a note about how to
    configure Airflow's webserver base URL to make the logs shareable.

    Arguments:
        message (Optional[str]): DAG-specific note appended to the alert body
            (e.g. "This may indicate a Zenodo API schema change").
        conn_id_param (str): DAG param name holding the Synapse connection id.
        user_list_param (str): DAG param name holding the comma-separated list of
            recipients (Synapse usernames or numeric ids).

    Returns:
        Callable[[Dict[str, Any]], None]: A callback for on_failure_callback.
    """

    def _callback(context: Dict[str, Any]) -> None:
        params = context.get("params", {})
        task_instance = context.get("task_instance")
        exception = context.get("exception")
        task_id = getattr(task_instance, "task_id", "unknown_task")
        log_url = getattr(task_instance, "log_url", "")
        dag_id = getattr(
            task_instance,
            "dag_id",
            getattr(context.get("dag"), "dag_id", "unknown_dag"),
        )
        run_id = getattr(task_instance, "run_id", context.get("run_id", "unknown_run"))
        # logical_date is the modern key; fall back to execution_date for older runs.
        execution_date = (
            context.get("logical_date") or context.get("execution_date") or ""
        )

        subject = f"Airflow DAG failure: {dag_id}"
        body = (
            f"The DAG '{dag_id}' failed on task '{task_id}'.\n\n"
            f"Run ID: {run_id}\n"
            f"Execution date: {execution_date}\n"
            f"Exception: {exception}\n"
        )
        if log_url and is_shareable_url(log_url):
            body += f"Logs: {log_url}"
        else:
            body += (
                "Logs: Not available as a shareable URL in this environment.\n"
                "If you're running Airflow locally or in GitHub Codespaces, view the task "
                "logs directly in the Airflow UI or inspect the Airflow container logs.\n\n"
                "Github Codespaces (Optional) \n"
                "-----------------------------------\n"
                "To generate shareable log URLs in failure notifications, configure Airflow's "
                "webserver base URL before starting Airflow:\n\n"
                'export AIRFLOW__WEBSERVER__BASE_URL="https://YOUR_CODESPACE_NAME-8080.app.github.dev"\n\n'
                "Replace YOUR_CODESPACE_NAME with the name of your Codespace, then restart "
                "the Airflow webserver.\n"
            )
        if message:
            if message:
                body += (
                    "\nAdditional Context\n"
                    "------------------------\n"
                    f"{message}\n"
                )

        try:
            send_synapse_message(
                conn_id=params.get(conn_id_param, ""),
                usernames=params.get(user_list_param, "").split(","),
                subject=subject,
                body=body,
            )
        except Exception:
            logger.exception("Failed to send Synapse failure alert.")

    return _callback

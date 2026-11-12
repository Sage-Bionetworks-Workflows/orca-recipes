"""Shared helpers for sending Synapse message alerts from any DAG.

Synapse delivers messages as email, so this is a dependency-free way for a DAG to
alert people (no SMTP setup required). The main entry points are:

- send_synapse_message - sends a custom message via email to Synapse users
  and/or teams.
- synapse_failure_callback - build on_failure_callback for a DAG that emails a
  standard failure alert to the DAG's configured recipients, with an optional
  DAG specific note

See CONTRIBUTION.md section DAG Failure Alerts for more info and usage examples.
"""
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from src.synapse_hook import SynapseHook
from src.utils import get_logger

logger = get_logger(__name__)

def is_shareable_url(url: str) -> bool:
    """Check if a URL is shareable (not localhost or private IP).

    Args:
        url (str): The URL to check

    Returns:
        bool: True if the URL is shareable, False otherwise
    """
    hostname = urlparse(url).hostname
    return hostname not in {"localhost", "127.0.0.1", "0.0.0.0", None}


def _clean_ids(ids: Optional[List[str]]) -> List[str]:
    """Strip whitespace from each id and drop blank entries.

    Args:
        ids (Optional[List[str]]): Raw ids, e.g. from splitting a comma-separated param

    Returns:
        List[str]: The non-blank, stripped ids
    """
    return [i.strip() for i in ids or [] if i.strip()]


def send_synapse_message(
    conn_id: str,
    subject: str,
    body: str,
    *,
    usernames: Optional[List[str]] = None,
    team_ids: Optional[List[str]] = None,
) -> None:
    """Send a Synapse message (delivered as email) to users and/or teams.

    At least one of usernames or team_ids must contain a recipient. To message a
    team, the account behind conn_id must be allowed to message that team.

    Arguments:
        conn_id (str): Synapse connection id
        subject (str): Message subject
        body (str): Message body
        usernames (Optional[List[str]]): Synapse usernames or numeric owner ids to
            message. Each is resolved to an owner id via its user profile.
        team_ids (Optional[List[str]]): Numeric Synapse team ids to message. These
            are sent to Synapse as-is (a team has no user profile to resolve).

    Raises:
        ValueError: If neither usernames nor team_ids contains a recipient.
    """
    usernames = _clean_ids(usernames)
    team_ids = _clean_ids(team_ids)
    if not usernames and not team_ids:
        raise ValueError(
            "At least one of usernames or team_ids must be provided; "
            f"no recipients for message: {subject}"
        )
    client = SynapseHook(conn_id).client
    owner_ids = [client.getUserProfile(u).get("ownerId") for u in usernames]
    response = client.sendMessage(owner_ids + team_ids, subject, body)
    logger.info(
        f"Sent Synapse message id={response['id']} "
        f"subject={response['subject']!r} "
        f"to {len(usernames)} user(s) and {len(team_ids)} team(s)"
    )


def synapse_failure_callback(
    message: Optional[str] = None,
    conn_id_param: str = "synapse_conn_id",
    user_list_param: str = "dev_user_list",
    team_list_param: str = "dev_team_list",
) -> Callable[[Dict[str, Any]], None]:
    """Build a on_failure_callback for a DAG that emails a Synapse failure alert.

    The returned callback reads the Synapse connection id and the user and team
    recipient lists from the DAG's params (so each DAG controls who gets alerted,
    e.g. a team on dev and users on prod via by_environment), builds a
    standard failure message (DAG id, task id, run id, execution date, exception,
    and a link to the task logs), and optionally appends a DAG-specific note.

    The send is wrapped in a try/except so a notification failure can never mask
    the original task error.

    If the task logs are not available as a shareable URL (e.g. when running Airflow
    locally), the alert will include guidance to view the logs directly in the
    Airflow UI or under $AIRFLOW_HOME/logs.

    Arguments:
        message (Optional[str]): DAG-specific note appended to the alert body
            (e.g. "This may indicate a Zenodo API schema change").
        conn_id_param (str): DAG param name holding the Synapse connection id.
        user_list_param (str): DAG param name holding the comma-separated list of
            recipients (Synapse usernames or numeric ids).
        team_list_param (str): DAG param name holding the comma-separated list of
            Synapse team ids. Either list may be empty or unset, but not both.

    Returns:
        Callable[[Dict[str, Any]], None]: A callback for on_failure_callback.
    """

    def _callback(context: Dict[str, Any]) -> None:
        """Send a Synapse failure alert for a failed task.

        Args:
            context (Dict[str, Any]): The context dictionary
                provided by Airflow to the callback,
                containing task instance, exception,
                and other relevant information.

        """
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
        # Airflow 3 removed execution_date, and logical_date can be None.
        logical_date = context.get("logical_date") or ""

        subject = f"Airflow DAG failure: {dag_id}"
        body = (
            f"The DAG '{dag_id}' failed on task '{task_id}'.\n\n"
            f"Run ID: {run_id}\n"
            f"Logical date: {logical_date}\n"
            f"Exception: {exception}\n"
        )
        if log_url and is_shareable_url(log_url):
            body += f"Logs: {log_url}"
        else:
            body += (
                "Logs: Not available as a shareable URL in this environment.\n"
                "If you're running Airflow locally, view the task logs directly in the "
                "Airflow UI or under $AIRFLOW_HOME/logs.\n"
            )
        if message:
            body += (
                "\nAdditional Context\n"
                "------------------------\n"
                f"{message}\n"
            )

        try:
            send_synapse_message(
                conn_id=params.get(conn_id_param, ""),
                subject=subject,
                body=body,
                usernames=(params.get(user_list_param) or "").split(","),
                team_ids=(params.get(team_list_param) or "").split(","),
            )
        except Exception:
            logger.exception("Failed to send Synapse failure alert.")

    return _callback

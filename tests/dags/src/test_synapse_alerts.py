"""Unit tests for the shared Synapse alert helpers (dags/src/synapse_alerts.py)."""
import logging

import pytest

from src import synapse_alerts


def test_send_synapse_message(monkeypatch, caplog):
    sent = {}

    class MockClient:
        def getUserProfile(self, username):
            return {"ownerId": f"owner-{username}"}

        def sendMessage(self, owner_ids, subject, body):
            sent["owner_ids"] = owner_ids
            sent["subject"] = subject
            sent["body"] = body

            return {
                "id": "message-123",
                "recipients": owner_ids,
                "subject": subject,
            }

    class MockSynapseHook:
        def __init__(self, conn_id):
            self.conn_id = conn_id
            self.client = MockClient()

    monkeypatch.setattr(synapse_alerts, "SynapseHook", MockSynapseHook)

    with caplog.at_level(logging.INFO):
        synapse_alerts.send_synapse_message(
            conn_id="synapse_conn",
            usernames=["alice", " bob ", ""],
            subject="Test subject",
            body="Test body",
        )

    assert sent == {
        "owner_ids": ["owner-alice", "owner-bob"],
        "subject": "Test subject",
        "body": "Test body",
    }

    assert (
        "Sent Synapse message id=message-123 "
        "subject='Test subject' to 2 user(s) and 0 team(s)"
    ) in caplog.text


@pytest.mark.parametrize(
    "recipients",
    [
        {},
        {"usernames": None, "team_ids": None},
        {"usernames": ["", "  "]},
        {"usernames": [""], "team_ids": [" "]},
    ],
    ids=["neither passed", "both None", "blank usernames", "both blank"],
)
def test_send_synapse_message_raises_when_no_recipients(monkeypatch, recipients):
    called = []

    class MockSynapseHook:
        def __init__(self, conn_id):
            called.append(conn_id)

    monkeypatch.setattr(synapse_alerts, "SynapseHook", MockSynapseHook)

    with pytest.raises(ValueError, match="At least one of usernames or team_ids"):
        synapse_alerts.send_synapse_message(
            conn_id="conn", subject="subj", body="body", **recipients
        )

    assert called == []


class MockTeamAwareClient:
    """Fails like Synapse would if a team id were looked up as a user profile."""

    def __init__(self, team_ids):
        self.team_ids = set(team_ids)
        self.requested_profiles = []
        self.sent = {}

    def getUserProfile(self, username):
        if username in self.team_ids:
            raise AssertionError(f"team id {username} looked up as a user profile")
        self.requested_profiles.append(username)
        return {"ownerId": f"owner-{username}"}

    def sendMessage(self, recipients, subject, body):
        self.sent = {"recipients": recipients, "subject": subject, "body": body}
        return {"id": "message-789", "recipients": recipients, "subject": subject}


def _patch_hook(monkeypatch, client):
    class MockSynapseHook:
        def __init__(self, conn_id):
            self.client = client

    monkeypatch.setattr(synapse_alerts, "SynapseHook", MockSynapseHook)


def test_send_synapse_message_to_teams_only(monkeypatch, caplog):
    client = MockTeamAwareClient(team_ids=["3622772"])
    _patch_hook(monkeypatch, client)

    with caplog.at_level(logging.INFO):
        synapse_alerts.send_synapse_message(
            conn_id="conn",
            subject="subj",
            body="body",
            team_ids=[" 3622772 ", ""],
        )

    assert client.requested_profiles == []
    assert client.sent["recipients"] == ["3622772"]
    assert "to 0 user(s) and 1 team(s)" in caplog.text


def test_send_synapse_message_to_users_and_teams(monkeypatch, caplog):
    client = MockTeamAwareClient(team_ids=["3622772"])
    _patch_hook(monkeypatch, client)

    with caplog.at_level(logging.INFO):
        synapse_alerts.send_synapse_message(
            conn_id="conn",
            subject="subj",
            body="body",
            usernames=["alice", ""],
            team_ids=["3622772"],
        )

    assert client.requested_profiles == ["alice"]
    assert client.sent["recipients"] == ["owner-alice", "3622772"]
    assert "to 1 user(s) and 1 team(s)" in caplog.text


def test_send_synapse_message_accepts_numeric_owner_ids(monkeypatch):
    requested_profiles = []
    sent = {}

    class MockClient:
        def getUserProfile(self, username):
            requested_profiles.append(username)
            return {"ownerId": username}

        def sendMessage(self, owner_ids, subject, body):
            sent["owner_ids"] = owner_ids

            return {
                "id": "message-456",
                "recipients": owner_ids,
                "subject": subject,
            }

    class MockSynapseHook:
        def __init__(self, conn_id):
            self.client = MockClient()

    monkeypatch.setattr(synapse_alerts, "SynapseHook", MockSynapseHook)

    synapse_alerts.send_synapse_message(
        conn_id="conn",
        usernames=["12345", "67890"],
        subject="Test subject",
        body="Test body",
    )

    assert requested_profiles == ["12345", "67890"]
    assert sent["owner_ids"] == ["12345", "67890"]


class MockTaskInstance:
    task_id = "fetch_metrics"
    dag_id = "some_dag"
    run_id = "manual__2025-01-01"
    log_url = "https://airflow/log"


def test_failure_callback_sends_alert_with_custom_message(monkeypatch):
    sent = {}

    def mock_send(conn_id, subject, body, *, usernames=None, team_ids=None):
        sent.update(
            conn_id=conn_id,
            usernames=usernames,
            team_ids=team_ids,
            subject=subject,
            body=body,
        )

    monkeypatch.setattr(synapse_alerts, "send_synapse_message", mock_send)

    callback = synapse_alerts.synapse_failure_callback(message="Check the API schema.")
    callback(
        {
            "params": {
                "synapse_conn_id": "synapse_conn",
                "dev_user_list": "alice,bob",
            },
            "task_instance": MockTaskInstance(),
            "exception": RuntimeError("boom"),
            "logical_date": "2025-01-01",
        }
    )

    assert sent["conn_id"] == "synapse_conn"
    assert sent["usernames"] == ["alice", "bob"]
    # dev_team_list unset - send_synapse_message drops the blank entry
    assert sent["team_ids"] == [""]
    assert "some_dag" in sent["subject"]
    assert "fetch_metrics" in sent["body"]
    assert "boom" in sent["body"]
    assert "Check the API schema." in sent["body"]  # custom message appended


def test_failure_callback_reads_custom_param_names(monkeypatch):
    sent = {}

    def mock_send(conn_id, subject, body, *, usernames=None, team_ids=None):
        sent.update(conn_id=conn_id, usernames=usernames, team_ids=team_ids)

    monkeypatch.setattr(synapse_alerts, "send_synapse_message", mock_send)

    callback = synapse_alerts.synapse_failure_callback(
        conn_id_param="my_conn",
        user_list_param="my_alertees",
        team_list_param="my_teams",
    )
    callback(
        {
            "params": {
                "my_conn": "conn-x",
                "my_alertees": "carol",
                "my_teams": "111,222",
            },
            "task_instance": MockTaskInstance(),
            "exception": ValueError("nope"),
        }
    )

    assert sent["conn_id"] == "conn-x"
    assert sent["usernames"] == ["carol"]
    assert sent["team_ids"] == ["111", "222"]


def test_failure_callback_sends_to_dev_team_list(monkeypatch):
    client = MockTeamAwareClient(team_ids=["3622772"])
    _patch_hook(monkeypatch, client)

    callback = synapse_alerts.synapse_failure_callback()
    callback(
        {
            "params": {
                "synapse_conn_id": "c",
                "dev_user_list": "",
                "dev_team_list": "3622772",
            },
            "task_instance": MockTaskInstance(),
            "exception": RuntimeError("boom"),
        }
    )

    assert client.requested_profiles == []
    assert client.sent["recipients"] == ["3622772"]
    assert "some_dag" in client.sent["subject"]


def test_failure_callback_logs_when_no_recipients(monkeypatch, caplog):
    called = []

    class MockSynapseHook:
        def __init__(self, conn_id):
            called.append(conn_id)

    monkeypatch.setattr(synapse_alerts, "SynapseHook", MockSynapseHook)

    callback = synapse_alerts.synapse_failure_callback()
    # Must not raise - the ValueError for missing recipients is logged instead.
    with caplog.at_level(logging.ERROR):
        callback(
            {
                "params": {
                    "synapse_conn_id": "c",
                    "dev_user_list": "",
                    "dev_team_list": "",
                },
                "task_instance": MockTaskInstance(),
                "exception": RuntimeError("boom"),
            }
        )

    assert called == []
    assert "Failed to send Synapse failure alert." in caplog.text


def test_failure_callback_swallows_send_errors(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("synapse unavailable")

    monkeypatch.setattr(synapse_alerts, "send_synapse_message", boom)

    callback = synapse_alerts.synapse_failure_callback()
    # Must not raise - a notification failure can't mask the original task error.
    callback(
        {
            "params": {"synapse_conn_id": "c", "dev_user_list": "alice"},
            "task_instance": MockTaskInstance(),
            "exception": RuntimeError("boom"),
        }
    )

class MockLocalTaskInstance:
    task_id = "fetch_metrics"
    dag_id = "some_dag"
    run_id = "manual__2025-01-01"
    log_url = "http://localhost:8080/dags/some_dag/grid?tab=logs"


def test_failure_callback_replaces_localhost_log_url_with_guidance(monkeypatch):
    sent = {}

    def mock_send(conn_id, subject, body, *, usernames=None, team_ids=None):
        sent.update(conn_id=conn_id, usernames=usernames, subject=subject, body=body)

    monkeypatch.setattr(synapse_alerts, "send_synapse_message", mock_send)

    callback = synapse_alerts.synapse_failure_callback()
    callback(
        {
            "params": {
                "synapse_conn_id": "synapse_conn",
                "dev_user_list": "alice",
            },
            "task_instance": MockLocalTaskInstance(),
            "exception": RuntimeError("boom"),
            "logical_date": "2025-01-01",
        }
    )

    assert "http://localhost:8080" not in sent["body"]
    assert "Not available as a shareable URL in this environment." in sent["body"]

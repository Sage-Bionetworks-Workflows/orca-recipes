"""Unit tests for the shared Synapse alert helpers (dags/src/synapse_alerts.py)."""
import logging

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
        "subject='Test subject' to 2 recipient(s)"
    ) in caplog.text


def test_send_synapse_message_skips_when_no_users(monkeypatch, caplog):
    called = []

    class MockSynapseHook:
        def __init__(self, conn_id):
            called.append(conn_id)

    monkeypatch.setattr(synapse_alerts, "SynapseHook", MockSynapseHook)

    with caplog.at_level(logging.WARNING):
        synapse_alerts.send_synapse_message(
            conn_id="conn",
            usernames=["", "  "],
            subject="subj",
            body="body",
        )

    assert called == []
    assert "No Synapse users provided; skipping message: subj" in caplog.text


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

    def mock_send(conn_id, usernames, subject, body):
        sent.update(conn_id=conn_id, usernames=usernames, subject=subject, body=body)

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
    assert "some_dag" in sent["subject"]
    assert "fetch_metrics" in sent["body"]
    assert "boom" in sent["body"]
    assert "Check the API schema." in sent["body"]  # custom message appended


def test_failure_callback_reads_custom_param_names(monkeypatch):
    sent = {}

    def mock_send(conn_id, usernames, subject, body):
        sent.update(conn_id=conn_id, usernames=usernames)

    monkeypatch.setattr(synapse_alerts, "send_synapse_message", mock_send)

    callback = synapse_alerts.synapse_failure_callback(
        conn_id_param="my_conn", user_list_param="my_alertees"
    )
    callback(
        {
            "params": {"my_conn": "conn-x", "my_alertees": "carol"},
            "task_instance": MockTaskInstance(),
            "exception": ValueError("nope"),
        }
    )

    assert sent["conn_id"] == "conn-x"
    assert sent["usernames"] == ["carol"]


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

    def mock_send(conn_id, usernames, subject, body):
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

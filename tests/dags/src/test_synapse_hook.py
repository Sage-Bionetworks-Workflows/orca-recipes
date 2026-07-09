"""Unit tests for the shared SynapseHook (dags/src/synapse_hook.py).

External dependencies (the Synapse client, the module-level query helper, and
Airflow's BaseHook) are mocked so the tests need no Synapse credentials or
Airflow metadata database.
"""
import airflow.hooks.base as base

import pytest

from src import synapse_hook
from src.synapse_hook import SynapseHook


def test_client_is_lazy_and_cached(monkeypatch):
    sentinel = object()
    login_calls = []

    def mock_login(self):
        login_calls.append(1)
        return sentinel

    monkeypatch.setattr(SynapseHook, "_login", mock_login)

    hook = SynapseHook("conn")
    assert hook._client is None  # not created until first access
    assert hook.client is sentinel
    assert hook.client is sentinel  # cached, not re-logged-in
    assert len(login_calls) == 1


def test_resolve_token_prefers_airflow_connection(monkeypatch):
    mock_token = "unit-test-token"

    class MockConnection:
        def __init__(self, token):
            self.password = token

    monkeypatch.setattr(
        base.BaseHook,
        "get_connection",
        classmethod(lambda cls, conn_id: MockConnection(mock_token)),
    )

    assert SynapseHook("MY_CONN")._resolve_token() == mock_token


def test_resolve_token_falls_back_to_env(monkeypatch):
    def mock_get_connection(cls, conn_id):
        raise RuntimeError("no such connection")

    monkeypatch.setattr(
        base.BaseHook, "get_connection", classmethod(mock_get_connection)
    )
    monkeypatch.setenv("SYNAPSE_AUTH_TOKEN", "env-token")

    assert SynapseHook("MISSING_CONN")._resolve_token() == "env-token"


def test_resolve_token_raises_when_unresolvable(monkeypatch):
    def mock_get_connection(cls, conn_id):
        raise RuntimeError("no such connection")

    monkeypatch.setattr(
        base.BaseHook, "get_connection", classmethod(mock_get_connection)
    )
    monkeypatch.delenv("SYNAPSE_AUTH_TOKEN", raising=False)

    with pytest.raises(EnvironmentError, match="SYNAPSE_AUTH_TOKEN"):
        SynapseHook("MISSING_CONN")._resolve_token()


def test_login_authenticates_with_resolved_token(monkeypatch):
    logged_in = {}

    class MockSynapse:
        def login(self, authToken=None, silent=None):
            logged_in["token"] = authToken
            logged_in["silent"] = silent

    monkeypatch.setattr(synapse_hook.synapseclient, "Synapse", MockSynapse)

    hook = SynapseHook("conn")
    monkeypatch.setattr(hook, "_resolve_token", lambda: "resolved-token")

    client = hook._login()
    assert isinstance(client, MockSynapse)
    assert logged_in == {"token": "resolved-token", "silent": True}


def test_get_submissions_with_status(monkeypatch):
    class MockSeries:
        def tolist(self):
            return ["syn1", "syn2"]

    class MockDataFrame:
        def __getitem__(self, key):
            return MockSeries()

    queries = []

    def mock_query(sql, synapse_client=None):
        queries.append(sql)
        return MockDataFrame()

    monkeypatch.setattr(synapse_hook, "query", mock_query)

    hook = SynapseHook("conn")
    hook._client = object()  # avoid a real login

    result = hook.get_submissions_with_status("syn123", submission_status="RECEIVED")
    assert result == ["syn1", "syn2"]
    # The second query filters the view by the requested status.
    assert "status = 'RECEIVED'" in queries[-1]


def test_update_submission_status(monkeypatch):
    class MockStatus:
        def __init__(self):
            self.status = None

    class MockClient:
        def __init__(self):
            self.stored = None
            self._status = MockStatus()

        def getSubmissionStatus(self, submission_id):
            self._status.submission_id = submission_id
            return self._status

        def store(self, status):
            self.stored = status

    hook = SynapseHook("conn")
    client = MockClient()
    hook._client = client

    hook.update_submission_status("9999", "SCORED")
    assert client.stored is client._status
    assert client.stored.status == "SCORED"


@pytest.mark.parametrize("bad_id", [1.5, ["x"], {"a": 1}, None])
def test_update_submission_status_rejects_non_str_int(bad_id):
    hook = SynapseHook("conn")
    hook._client = object()  # the type check happens before the client is used
    with pytest.raises(TypeError, match="string or int"):
        hook.update_submission_status(bad_id, "SCORED")

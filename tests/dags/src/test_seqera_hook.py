"""Unit tests for the shared SeqeraHook (dags/src/seqera_hook.py).

Everything external is mocked (Airflow's BaseHook, and the HTTP layer), so these
tests need no Seqera credentials, no network access, and no Airflow metadata
database.
"""
from datetime import datetime, timezone

import airflow.hooks.base as base
import pytest
from requests.exceptions import HTTPError

from src import seqera_hook
from src.seqera_hook import (
    ComputeEnv,
    Label,
    LaunchInfo,
    SeqeraClient,
    SeqeraConfig,
    SeqeraHook,
    Workflow,
    WorkflowState,
    WorkflowStatus,
    WorkflowTask,
    Workspace,
    dedup,
    increment_suffix,
    parse_datetime,
)


WORKSPACE = "sage-bionetworks/example-project"
WORKSPACE_ID = 12345


class MockConnection:
    """Stand-in for an Airflow connection with a Seqera-style layout."""

    def __init__(
        self,
        host="tower.sagebionetworks.org",
        schema="api",
        password="conn-token",
        extra_dejson=None,
    ):
        self.host = host
        self.schema = schema
        self.password = password
        self.extra_dejson = (
            {"workspace": WORKSPACE} if extra_dejson is None else extra_dejson
        )


class MockClient:
    """Records requests and replays canned responses keyed by path.

    GET and POST responses are kept apart because some Seqera endpoints (such
    as /labels) are used with both methods.
    """

    def __init__(self, responses=None, post_responses=None):
        self.responses = responses or {}
        self.post_responses = post_responses or {}
        self.calls = []
        self.posts = []

    def get(self, path, **kwargs):
        self.calls.append((path, kwargs.get("params")))
        return self.responses[path]

    def post(self, path, **kwargs):
        self.posts.append((path, kwargs.get("params"), kwargs.get("json")))
        return self.post_responses[path]

    def get_items(self, path, items_key, params=None):
        self.calls.append((path, params))
        return self.responses[path][items_key]

    def unwrap(self, json, key):
        return SeqeraClient.unwrap(self, json, key)


def make_hook(responses=None, post_responses=None, workspace=WORKSPACE):
    """Build a hook wired to a MockClient, bypassing credential resolution."""
    hook = SeqeraHook("TEST_TOWER_CONN")
    hook._config = SeqeraConfig("https://tower.example.org/api", "token", workspace)
    hook._client = MockClient(responses, post_responses)
    hook._workspace_id = WORKSPACE_ID
    return hook


def disable_airflow_connections(monkeypatch):
    """Make BaseHook.get_connection fail, as it does outside Airflow."""

    def mock_get_connection(cls, conn_id):
        raise RuntimeError("no such connection")

    monkeypatch.setattr(
        base.BaseHook, "get_connection", classmethod(mock_get_connection)
    )


def clear_tower_env(monkeypatch):
    """Remove every TOWER_* variable the hook falls back on."""
    for name in ("TOWER_ACCESS_TOKEN", "TOWER_API_ENDPOINT", "TOWER_WORKSPACE"):
        monkeypatch.delenv(name, raising=False)


# --- Helper functions -------------------------------------------------------


def test_dedup_preserves_order():
    # Profile order decides which Nextflow setting wins, so it must survive.
    assert dedup(["tower", "aws_prod", "tower", "debug"]) == [
        "tower",
        "aws_prod",
        "debug",
    ]


@pytest.mark.parametrize(
    "text, expected",
    [
        ("foo", "foo_2"),
        ("foo_", "foo_2"),
        ("foo_1", "foo_2"),
        ("foo_9", "foo_10"),
        ("foo_bar", "foo_bar_2"),
    ],
)
def test_increment_suffix(text, expected):
    assert increment_suffix(text) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-01-02T03:04:05Z", datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
        (
            "2026-01-02T03:04:05.123Z",
            datetime(2026, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc),
        ),
        # Naive datetimes are assumed to already be UTC.
        (
            datetime(2026, 1, 2, 3, 4, 5),
            datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        ),
        (None, None),
        ("", None),
        # Unparseable timestamps degrade to None instead of failing a task.
        ("not-a-timestamp", None),
    ],
)
def test_parse_datetime(value, expected):
    assert parse_datetime(value) == expected


# --- Models -----------------------------------------------------------------


@pytest.mark.parametrize(
    "state, is_done, is_successful",
    [
        (WorkflowState.SUBMITTED, False, False),
        (WorkflowState.RUNNING, False, False),
        (WorkflowState.SUCCEEDED, True, True),
        (WorkflowState.FAILED, True, False),
        (WorkflowState.CANCELLED, True, False),
        (WorkflowState.UNKNOWN, True, False),
    ],
)
def test_workflow_status(state, is_done, is_successful):
    status = WorkflowStatus(state=state)
    assert status.is_done is is_done
    assert status.is_successful is is_successful


def test_workflow_status_accepts_raw_string():
    assert WorkflowStatus(state="RUNNING").state is WorkflowState.RUNNING


def test_workflow_status_rejects_unrecognized_state():
    # Failing loudly beats treating an in-flight run as finished.
    with pytest.raises(ValueError):
        WorkflowStatus(state="PENDING")


def test_workflow_from_json():
    workflow = Workflow.from_json(
        {
            "id": "1abc",
            "runName": "airflow-agora-model-ad",
            "status": "SUCCEEDED",
            "sessionId": "session-1",
            "userName": "orca-service",
            "projectName": "Sage-Bionetworks-Workflows/nf-agora",
            "workDir": "s3://bucket/work",
            "params": {"dataset": "model_details"},
            "commitId": "d34db33f",
            "submit": "2026-01-01T00:00:00Z",
            "complete": "2026-01-01T01:30:00Z",
            "unexpectedKey": "ignored",
        }
    )

    assert workflow.id == "1abc"
    assert workflow.run_name == "airflow-agora-model-ad"
    assert workflow.state is WorkflowState.SUCCEEDED
    assert workflow.status.is_successful
    assert workflow.session_id == "session-1"
    assert workflow.username == "orca-service"
    assert workflow.project_name == "Sage-Bionetworks-Workflows/nf-agora"
    assert workflow.work_dir == "s3://bucket/work"
    assert workflow.params == {"dataset": "model_details"}
    assert workflow.commit_id == "d34db33f"
    assert workflow.complete - workflow.submit == (
        datetime(2026, 1, 1, 1, 30, tzinfo=timezone.utc)
        - datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    )
    assert workflow.raw["unexpectedKey"] == "ignored"


def test_workflow_task_from_json():
    task = WorkflowTask.from_json(
        {
            "taskId": 7,
            "status": "FAILED",
            "name": "PROCESS (1)",
            "exitStatus": 137,
            # Seqera spells this one all-lowercase on task payloads.
            "workdir": "s3://bucket/work/ab/cdef",
            "machineType": "m5.large",
        }
    )
    assert task.task_id == 7
    assert task.exit_status == 137
    assert task.work_dir == "s3://bucket/work/ab/cdef"
    assert task.machine_type == "m5.large"


def test_compute_env_from_json_summary_and_detail():
    summary = ComputeEnv.from_json(
        {
            "id": "ce-1",
            "name": "project-ondemand-v13",
            "status": "AVAILABLE",
            "workDir": "s3://bucket/work",
        }
    )
    assert summary.work_dir == "s3://bucket/work"
    assert summary.labels == []

    detail = ComputeEnv.from_json(
        {
            "id": "ce-1",
            "name": "project-ondemand-v13",
            "dateCreated": "2026-01-01T00:00:00Z",
            "config": {
                "workDir": "s3://bucket/nested-work",
                "preRunScript": "echo hello",
            },
            "labels": [{"id": 1, "name": "cost-center", "resource": True}],
        }
    )
    assert detail.work_dir == "s3://bucket/nested-work"
    assert detail.pre_run_script == "echo hello"
    assert detail.date_created == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert detail.labels == [Label(id=1, name="cost-center", resource=True)]


def test_workspace_full_name_is_lowercased():
    workspace = Workspace.from_json(
        {
            "workspaceId": 1,
            "workspaceName": "Example-Project",
            "orgName": "Sage-Bionetworks",
        }
    )
    assert workspace.full_name == "sage-bionetworks/example-project"


# --- LaunchInfo -------------------------------------------------------------


def test_launch_info_to_json():
    info = LaunchInfo(
        run_name="run-1",
        pipeline="Sage-Bionetworks-Workflows/nf-agora",
        revision="main",
        work_dir="s3://bucket/work",
        compute_env_id="ce-1",
        profiles=["tower", "aws_prod", "tower"],
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        label_ids=[2, 1, 2],
        params={"dataset": "model_details"},
    )

    payload = info.to_json()["launch"]

    assert payload["runName"] == "run-1"
    assert payload["pipeline"] == "Sage-Bionetworks-Workflows/nf-agora"
    assert payload["revision"] == "main"
    assert payload["workDir"] == "s3://bucket/work"
    assert payload["computeEnvId"] == "ce-1"
    assert payload["configProfiles"] == ["tower", "aws_prod"]
    assert payload["workspaceSecrets"] == ["SYNAPSE_AUTH_TOKEN"]
    assert payload["labelIds"] == [2, 1]
    assert payload["paramsText"] == '{"dataset": "model_details"}'
    assert payload["entryName"] == ""
    # Resume fields are only sent when resuming.
    assert "resume" not in payload
    assert "sessionId" not in payload


def test_launch_info_to_json_includes_resume_fields():
    info = LaunchInfo(
        run_name="run-2",
        pipeline="pipe",
        work_dir="s3://bucket/work",
        compute_env_id="ce-1",
        resume=True,
        session_id="session-1",
    )
    payload = info.to_json()["launch"]
    assert payload["resume"] is True
    assert payload["sessionId"] == "session-1"


def test_launch_info_params_omitted_when_empty():
    info = LaunchInfo(
        run_name="run-3", pipeline="pipe", work_dir="s3://w", compute_env_id="ce-1"
    )
    assert info.to_json()["launch"]["paramsText"] == ""


def test_launch_info_rejects_resume_without_session_id():
    with pytest.raises(ValueError, match="session ID"):
        LaunchInfo(run_name="run", pipeline="pipe", resume=True)


def test_launch_info_to_json_requires_essential_fields():
    info = LaunchInfo(run_name="run", pipeline="pipe")
    with pytest.raises(ValueError, match="compute_env_id"):
        info.to_json()


def test_launch_info_fill_in_only_fills_falsy_values():
    info = LaunchInfo(run_name="run", pipeline="pipe", work_dir="s3://explicit")
    info.fill_in("work_dir", "s3://default")
    info.fill_in("pre_run_script", "echo hello")
    assert info.work_dir == "s3://explicit"
    assert info.pre_run_script == "echo hello"


def test_launch_info_add_in_dedups():
    info = LaunchInfo(label_ids=[1])
    info.add_in("label_ids", [2, 1, 3])
    assert info.label_ids == [1, 2, 3]

    with pytest.raises(ValueError, match="not a list"):
        info.add_in("run_name", ["nope"])


# --- Client -----------------------------------------------------------------


def test_client_request_is_authenticated_and_bounded(monkeypatch):
    captured = {}

    def mock_request(method, url, **kwargs):
        captured.update(method=method, url=url, **kwargs)
        return "response"

    monkeypatch.setattr(seqera_hook.requests, "request", mock_request)

    client = SeqeraClient("https://tower.example.org/api/", "secret")
    assert client.request("GET", "/user-info") == "response"

    assert captured["url"] == "https://tower.example.org/api/user-info"
    assert captured["headers"]["Authorization"] == "Bearer secret"
    assert captured["timeout"] == seqera_hook.REQUEST_TIMEOUT


def test_client_request_json_includes_response_body_on_error(monkeypatch):
    class MockResponse:
        text = '{"message": "Workspace not found"}'

        def raise_for_status(self):
            raise HTTPError("404 Client Error")

        def json(self):  # pragma: no cover - not reached
            return {}

    monkeypatch.setattr(
        seqera_hook.requests, "request", lambda *args, **kwargs: MockResponse()
    )

    client = SeqeraClient("https://tower.example.org/api", "secret")
    with pytest.raises(HTTPError, match="Workspace not found"):
        client.request_json("GET", "/workflow/1")


def test_client_unwrap_raises_on_missing_key():
    client = SeqeraClient("https://tower.example.org/api", "secret")
    assert client.unwrap({"workflow": {"id": "1"}}, "workflow") == {"id": "1"}
    with pytest.raises(HTTPError, match="Expecting 'workflow' key"):
        client.unwrap({}, "workflow")


def test_client_get_items_follows_pagination(monkeypatch):
    pages = [
        {"totalSize": 3, "workflows": [{"id": "1"}]},
        {"totalSize": 3, "workflows": [{"id": "2"}]},
        {"totalSize": 3, "workflows": [{"id": "3"}]},
    ]
    requested_params = []

    client = SeqeraClient("https://tower.example.org/api", "secret")

    def mock_get(path, params=None):
        # Copied: the client reuses one params dict across pages.
        requested_params.append(dict(params or {}))
        return pages[len(requested_params) - 1]

    monkeypatch.setattr(client, "get", mock_get)

    items = client.get_items("/workflow", "workflows", params={"workspaceId": 1})

    assert items == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    # The first request is unpaged; later ones page by offset.
    assert requested_params[0] == {"workspaceId": 1}
    assert [params["offset"] for params in requested_params[1:]] == [1, 2]


def test_client_get_items_returns_unpaged_response_as_is(monkeypatch):
    client = SeqeraClient("https://tower.example.org/api", "secret")
    monkeypatch.setattr(
        client, "get", lambda path, params=None: {"computeEnvs": [{"id": "ce-1"}]}
    )
    assert client.get_items("/compute-envs", "computeEnvs") == [{"id": "ce-1"}]


def test_client_get_items_stops_on_empty_page(monkeypatch):
    # A totalSize the endpoint never delivers must not spin forever.
    client = SeqeraClient("https://tower.example.org/api", "secret")
    pages = [{"totalSize": 10, "labels": [{"id": 1}]}, {"totalSize": 10, "labels": []}]
    monkeypatch.setattr(client, "get", lambda path, params=None: pages.pop(0))
    assert client.get_items("/labels", "labels") == [{"id": 1}]


# --- Credential resolution --------------------------------------------------


def test_config_prefers_airflow_connection(monkeypatch):
    monkeypatch.setattr(
        base.BaseHook,
        "get_connection",
        classmethod(lambda cls, conn_id: MockConnection()),
    )
    monkeypatch.setenv("TOWER_ACCESS_TOKEN", "env-token")

    config = SeqeraHook("MY_CONN").config
    assert config.api_endpoint == "https://tower.sagebionetworks.org/api"
    assert config.auth_token == "conn-token"
    assert config.workspace == WORKSPACE


def test_config_falls_back_to_env(monkeypatch):
    disable_airflow_connections(monkeypatch)
    clear_tower_env(monkeypatch)
    monkeypatch.setenv("TOWER_ACCESS_TOKEN", "env-token")
    monkeypatch.setenv("TOWER_API_ENDPOINT", "https://tower-dev.example.org/api/")
    monkeypatch.setenv("TOWER_WORKSPACE", WORKSPACE)

    config = SeqeraHook("MISSING_CONN").config
    assert config.auth_token == "env-token"
    assert config.api_endpoint == "https://tower-dev.example.org/api"
    assert config.workspace == WORKSPACE


def test_config_defaults_api_endpoint(monkeypatch):
    disable_airflow_connections(monkeypatch)
    clear_tower_env(monkeypatch)
    monkeypatch.setenv("TOWER_ACCESS_TOKEN", "env-token")

    config = SeqeraHook("MISSING_CONN").config
    assert config.api_endpoint == SeqeraHook.default_api_endpoint
    assert config.workspace is None


def test_config_raises_when_token_unresolvable(monkeypatch):
    disable_airflow_connections(monkeypatch)
    clear_tower_env(monkeypatch)

    with pytest.raises(EnvironmentError, match="TOWER_ACCESS_TOKEN"):
        SeqeraHook("MISSING_CONN").config


def test_config_rejects_malformed_workspace(monkeypatch):
    monkeypatch.setattr(
        base.BaseHook,
        "get_connection",
        classmethod(
            lambda cls, conn_id: MockConnection(extra_dejson={"workspace": "no-slash"})
        ),
    )
    with pytest.raises(ValueError, match="organization-name"):
        SeqeraHook("MY_CONN").config


def test_connection_without_host_leaves_endpoint_to_fallbacks(monkeypatch):
    monkeypatch.setattr(
        base.BaseHook,
        "get_connection",
        classmethod(lambda cls, conn_id: MockConnection(host=None)),
    )
    clear_tower_env(monkeypatch)
    assert SeqeraHook("MY_CONN").config.api_endpoint == SeqeraHook.default_api_endpoint


def test_client_is_lazy_and_cached(monkeypatch):
    monkeypatch.setattr(
        base.BaseHook,
        "get_connection",
        classmethod(lambda cls, conn_id: MockConnection()),
    )

    hook = SeqeraHook("MY_CONN")
    assert hook._client is None  # not created until first access
    client = hook.client
    assert client is hook.client  # cached
    assert client.auth_token == "conn-token"


def test_workspace_raises_when_unset():
    hook = make_hook(workspace=None)
    with pytest.raises(EnvironmentError, match="does not specify a workspace"):
        hook.workspace


def test_workspace_is_lowercased():
    hook = make_hook(workspace="Sage-Bionetworks/Example-Project")
    assert hook.workspace == WORKSPACE


# --- Workspace ID resolution ------------------------------------------------


WORKSPACES_RESPONSE = {
    "orgsAndWorkspaces": [
        # Organizations appear in this listing with a null workspace ID.
        {"workspaceId": None, "orgId": 1, "orgName": "Sage-Bionetworks"},
        {
            "workspaceId": WORKSPACE_ID,
            "workspaceName": "example-project",
            "orgName": "Sage-Bionetworks",
        },
    ]
}


def test_resolve_workspace_id():
    hook = make_hook(
        {
            "/user-info": {"user": {"id": 9, "userName": "orca-service"}},
            "/user/9/workspaces": WORKSPACES_RESPONSE,
        }
    )
    hook._workspace_id = None

    assert hook.workspace_id == WORKSPACE_ID
    # Cached: the second access makes no further requests.
    assert hook.workspace_id == WORKSPACE_ID
    assert len(hook._client.calls) == 2


def test_resolve_workspace_id_raises_when_unavailable():
    hook = make_hook(
        {
            "/user-info": {"user": {"id": 9, "userName": "orca-service"}},
            "/user/9/workspaces": WORKSPACES_RESPONSE,
        },
        workspace="sage-bionetworks/other-project",
    )
    hook._workspace_id = None

    with pytest.raises(ValueError, match="not available to user 'orca-service'"):
        hook.workspace_id


# --- Workflow operations ----------------------------------------------------


def test_get_workflow():
    hook = make_hook(
        {"/workflow/1abc": {"workflow": {"id": "1abc", "status": "RUNNING"}}}
    )
    workflow = hook.get_workflow("1abc")

    assert workflow.status.state is WorkflowState.RUNNING
    assert workflow.status.is_done is False
    # Every workspace-scoped request carries the workspace ID.
    assert hook._client.calls == [("/workflow/1abc", {"workspaceId": WORKSPACE_ID})]


def test_list_workflows_filters_on_launch_label():
    hook = make_hook({"/workflow": {"workflows": [{"workflow": {"id": "1"}}]}})
    workflows = hook.list_workflows("runName:foo")

    assert [workflow.id for workflow in workflows] == ["1"]
    _, params = hook._client.calls[0]
    assert params["search"] == f"runName:foo label:{SeqeraHook.launch_label}"


def test_list_previous_workflows_matches_pipeline_and_run_name_prefix():
    hook = make_hook(
        {
            "/workflow": {
                "workflows": [
                    {"workflow": {"id": "1", "projectName": "org/pipe", "runName": "run"}},
                    # Right pipeline, unrelated run name.
                    {"workflow": {"id": "2", "projectName": "org/pipe", "runName": "other"}},
                    # Right run name, different pipeline.
                    {"workflow": {"id": "3", "projectName": "org/nope", "runName": "run"}},
                    # Relaunches keep the run name as a prefix.
                    {"workflow": {"id": "4", "projectName": "org/pipe", "runName": "run_2"}},
                ]
            }
        }
    )
    info = LaunchInfo(run_name="run", pipeline="org/pipe")

    previous = hook.list_previous_workflows(info)
    assert [workflow.id for workflow in previous] == ["1", "4"]


def test_get_latest_previous_workflow_prefers_ongoing_run():
    hook = make_hook(
        {
            "/workflow": {
                "workflows": [
                    {
                        "workflow": {
                            "id": "old",
                            "projectName": "org/pipe",
                            "runName": "run",
                            "status": "SUCCEEDED",
                            "submit": "2026-01-01T00:00:00Z",
                        }
                    },
                    {
                        "workflow": {
                            "id": "ongoing",
                            "projectName": "org/pipe",
                            "runName": "run_2",
                            "status": "RUNNING",
                            "submit": "2026-01-02T00:00:00Z",
                        }
                    },
                ]
            }
        }
    )
    latest = hook.get_latest_previous_workflow(LaunchInfo(run_name="run", pipeline="org/pipe"))
    assert latest.id == "ongoing"


def test_get_latest_previous_workflow_falls_back_to_latest_submission():
    hook = make_hook(
        {
            "/workflow": {
                "workflows": [
                    {
                        "workflow": {
                            "id": "newer",
                            "projectName": "org/pipe",
                            "runName": "run_2",
                            "status": "FAILED",
                            "submit": "2026-01-02T00:00:00Z",
                        }
                    },
                    {
                        "workflow": {
                            "id": "older",
                            "projectName": "org/pipe",
                            "runName": "run",
                            "status": "SUCCEEDED",
                            "submit": "2026-01-01T00:00:00Z",
                        }
                    },
                    # A run with no submit timestamp must not break sorting.
                    {
                        "workflow": {
                            "id": "undated",
                            "projectName": "org/pipe",
                            "runName": "run_3",
                            "status": "CANCELLED",
                        }
                    },
                ]
            }
        }
    )
    latest = hook.get_latest_previous_workflow(LaunchInfo(run_name="run", pipeline="org/pipe"))
    assert latest.id == "newer"


def test_get_latest_previous_workflow_returns_none_without_previous_runs():
    hook = make_hook({"/workflow": {"workflows": []}})
    assert hook.get_latest_previous_workflow(LaunchInfo(run_name="run", pipeline="p")) is None


def test_get_latest_previous_workflow_rejects_multiple_ongoing_runs():
    hook = make_hook(
        {
            "/workflow": {
                "workflows": [
                    {
                        "workflow": {
                            "id": "a",
                            "projectName": "org/pipe",
                            "runName": "run",
                            "status": "RUNNING",
                        }
                    },
                    {
                        "workflow": {
                            "id": "b",
                            "projectName": "org/pipe",
                            "runName": "run_2",
                            "status": "SUBMITTED",
                        }
                    },
                ]
            }
        }
    )
    with pytest.raises(ValueError, match="Multiple ongoing workflow runs"):
        hook.get_latest_previous_workflow(LaunchInfo(run_name="run", pipeline="org/pipe"))


# --- Compute environments and labels ----------------------------------------


def test_get_latest_compute_env_filters_by_name():
    hook = make_hook(
        {
            "/compute-envs": {
                "computeEnvs": [
                    {"id": "ce-spot", "name": "project-spot", "status": "AVAILABLE"},
                    {"id": "ce-od", "name": "project-ondemand", "status": "AVAILABLE"},
                ]
            }
        }
    )
    assert hook.get_latest_compute_env("ondemand") == "ce-od"
    # Only the listing is needed when exactly one environment matches.
    assert [path for path, _ in hook._client.calls] == ["/compute-envs"]


def test_get_latest_compute_env_picks_most_recent_of_several():
    hook = make_hook(
        {
            "/compute-envs": {
                "computeEnvs": [
                    {"id": "ce-old", "name": "project-ondemand-v1", "status": "AVAILABLE"},
                    {"id": "ce-new", "name": "project-ondemand-v2", "status": "AVAILABLE"},
                ]
            },
            "/compute-envs/ce-old": {
                "computeEnv": {"id": "ce-old", "dateCreated": "2026-01-01T00:00:00Z"}
            },
            "/compute-envs/ce-new": {
                "computeEnv": {"id": "ce-new", "dateCreated": "2026-06-01T00:00:00Z"}
            },
        }
    )
    assert hook.get_latest_compute_env("ondemand") == "ce-new"


def test_get_latest_compute_env_raises_without_matches():
    hook = make_hook(
        {
            "/compute-envs": {
                "computeEnvs": [
                    {"id": "ce-spot", "name": "project-spot", "status": "AVAILABLE"}
                ]
            }
        }
    )
    with pytest.raises(ValueError, match="No available compute environments"):
        hook.get_latest_compute_env("ondemand")


def test_create_label_reuses_existing_non_resource_label():
    hook = make_hook(
        {
            "/labels": {
                "labels": [
                    # Resource labels are a different namespace and never reused.
                    {"id": 1, "name": SeqeraHook.launch_label, "resource": True},
                    {"id": 2, "name": SeqeraHook.launch_label, "resource": False},
                ]
            }
        }
    )
    assert hook.create_label(SeqeraHook.launch_label) == 2
    assert hook._client.posts == []


def test_create_label_creates_when_absent():
    hook = make_hook(
        responses={"/labels": {"labels": [{"id": 1, "name": "other", "resource": False}]}},
        post_responses={"/labels": {"id": 7, "name": "new-label", "resource": False}},
    )

    assert hook.create_label("new-label") == 7
    _, params, payload = hook._client.posts[0]
    assert params == {"workspaceId": WORKSPACE_ID}
    assert payload == {"name": "new-label", "resource": False}


def test_get_workflow_tasks_and_task_logs():
    hook = make_hook(
        {
            "/workflow/1abc/tasks": {"tasks": [{"task": {"taskId": 1, "status": "FAILED"}}]},
            "/workflow/1abc/log/1": {"log": {"entries": ["line one", "line two"]}},
        }
    )
    tasks = hook.get_workflow_tasks("1abc")
    assert [task.task_id for task in tasks] == [1]
    assert hook.get_task_logs("1abc", 1) == "line one\nline two"


# --- launch_workflow --------------------------------------------------------


LAUNCH_RESPONSES = {
    "/compute-envs": {
        "computeEnvs": [
            {"id": "ce-1", "name": "project-ondemand", "status": "AVAILABLE"}
        ]
    },
    "/compute-envs/ce-1": {
        "computeEnv": {
            "id": "ce-1",
            "name": "project-ondemand",
            "config": {
                "workDir": "s3://default-bucket/work",
                "preRunScript": "echo default",
            },
            "labels": [{"id": 10, "name": "cost-center", "resource": True}],
        }
    },
    "/labels": {"labels": [{"id": 20, "name": SeqeraHook.launch_label, "resource": False}]},
}

LAUNCH_POST_RESPONSES = {"/workflow/launch": {"workflowId": "new-run"}}


def test_launch_workflow_ignoring_previous_runs():
    hook = make_hook(dict(LAUNCH_RESPONSES), LAUNCH_POST_RESPONSES)
    info = LaunchInfo(
        run_name="run-1",
        pipeline="org/pipe",
        revision="main",
        profiles=["tower"],
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
    )

    run_id = hook.launch_workflow(info, "ondemand", ignore_previous_runs=True)

    assert run_id == "new-run"
    # No previous-run lookup happened.
    assert "/workflow" not in [path for path, _ in hook._client.calls]

    path, params, payload = hook._client.posts[-1]
    assert path == "/workflow/launch"
    assert params == {"workspaceId": WORKSPACE_ID}
    launch = payload["launch"]
    assert launch["computeEnvId"] == "ce-1"
    # Unset fields inherit the compute environment's defaults.
    assert launch["workDir"] == "s3://default-bucket/work"
    assert launch["preRunScript"] == "echo default"
    # The compute environment's resource labels plus the launch label.
    assert launch["labelIds"] == [10, 20]
    assert launch["configProfiles"] == ["tower"]


def test_launch_workflow_keeps_explicit_values():
    hook = make_hook(dict(LAUNCH_RESPONSES), LAUNCH_POST_RESPONSES)
    info = LaunchInfo(
        run_name="run-1",
        pipeline="org/pipe",
        work_dir="s3://explicit/work",
        pre_run_script="echo explicit",
    )

    hook.launch_workflow(info, "ondemand", ignore_previous_runs=True)

    launch = hook._client.posts[-1][2]["launch"]
    assert launch["workDir"] == "s3://explicit/work"
    assert launch["preRunScript"] == "echo explicit"


@pytest.mark.parametrize("state", ["SUCCEEDED", "UNKNOWN", "RUNNING", "SUBMITTED"])
def test_launch_workflow_returns_existing_run(state):
    responses = dict(LAUNCH_RESPONSES)
    responses["/workflow"] = {
        "workflows": [
            {
                "workflow": {
                    "id": "existing",
                    "projectName": "org/pipe",
                    "runName": "run-1",
                    "status": state,
                    "submit": "2026-01-01T00:00:00Z",
                }
            }
        ]
    }
    hook = make_hook(responses, LAUNCH_POST_RESPONSES)

    run_id = hook.launch_workflow(LaunchInfo(run_name="run-1", pipeline="org/pipe"), "ondemand")

    # Ongoing, succeeded, and unknown-state runs are reused, not duplicated.
    assert run_id == "existing"
    assert hook._client.posts == []


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_launch_workflow_resumes_failed_run(state):
    responses = dict(LAUNCH_RESPONSES)
    responses["/workflow"] = {
        "workflows": [
            {
                "workflow": {
                    "id": "failed-run",
                    "projectName": "org/pipe",
                    "runName": "run-1",
                    "status": state,
                    "sessionId": "session-1",
                    "submit": "2026-01-01T00:00:00Z",
                }
            }
        ]
    }
    hook = make_hook(responses, LAUNCH_POST_RESPONSES)
    info = LaunchInfo(run_name="run-1", pipeline="org/pipe")

    run_id = hook.launch_workflow(info, "ondemand")

    assert run_id == "new-run"
    launch = hook._client.posts[-1][2]["launch"]
    assert launch["resume"] is True
    assert launch["sessionId"] == "session-1"
    # The relaunch gets an incremented run name, keeping the prefix searchable.
    assert launch["runName"] == "run-1_2"


def test_launch_workflow_requires_run_name_and_pipeline():
    hook = make_hook(dict(LAUNCH_RESPONSES), LAUNCH_POST_RESPONSES)
    with pytest.raises(ValueError, match="'run_name' and 'pipeline'"):
        hook.launch_workflow(LaunchInfo(pipeline="org/pipe"), "ondemand")

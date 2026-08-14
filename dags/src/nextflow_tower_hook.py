"""Airflow hook for Seqera Platform (Nextflow Tower).

This is an in-repo replacement for ``orca.services.nextflowtower`` from py-orca,
which collapses that package's hook/ops/client/config layers into a single hook
(the same shape as ``src.synapse_hook.SynapseHook``) and swaps the pydantic v1
models for plain dataclasses.

Credentials resolve from an Airflow connection, with environment variable
fallbacks for local runs. The connection layout is unchanged from py-orca, so
existing connection secrets keep working:

    conn_type: tower
    host:      tower.sagebionetworks.org
    schema:    api                              # -> https://<host>/<schema>
    password:  <Nextflow Tower personal access token>
    extra:     {"workspace": "<organization>/<workspace>"}

Local fallbacks (used only when the connection cannot be resolved, e.g. under
``dag.test()`` without a metadata database) follow the names Nextflow Tower's
own CLI uses: ``TOWER_ACCESS_TOKEN``, ``TOWER_API_ENDPOINT``, and
``TOWER_WORKSPACE``.

Typical DAG usage:

    hook = NextflowTowerHook(context["params"]["tower_conn_id"])
    run_id = hook.launch_workflow(LaunchInfo(...), "ondemand")
    workflow = hook.get_workflow(run_id)
    workflow.status.is_done
"""

import json as json_module
import os
import re
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Optional, TypeVar

from airflow.exceptions import AirflowNotFoundException
import requests
from requests.exceptions import HTTPError

from src.utils import get_logger


logger = get_logger(__name__)

T = TypeVar("T")

# Number of items to request per page from Tower's paged endpoints.
PAGE_SIZE = 50

# Requests never block a worker slot forever; Nextflow Tower's API responds in seconds.
REQUEST_TIMEOUT = 30

# Sort fallback for runs and compute environments with no usable timestamp.
EPOCH = datetime.min.replace(tzinfo=timezone.utc)


class WorkflowState(str, Enum):
    """Valid values for the state of a Nextflow Tower workflow run."""

    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


# States a workflow run can no longer transition out of.
TERMINAL_WORKFLOW_STATES = frozenset(
    {
        WorkflowState.SUCCEEDED,
        WorkflowState.FAILED,
        WorkflowState.CANCELLED,
        WorkflowState.UNKNOWN,
    }
)


def dedup(items: Iterable[T]) -> list[T]:
    """Remove duplicates from an iterable, preserving order.

    Order matters for values such as Nextflow config profiles, where the last
    profile wins for any setting defined more than once.

    Args:
        items: Items to deduplicate.

    Returns:
        The items, in their original order, without duplicates.
    """
    return list(dict.fromkeys(items))


def increment_suffix(text: str, separator: str = "_") -> str:
    """Increment the integer suffix of a string.

    Used to derive a fresh run name when relaunching a previous workflow run
    (``foo`` -> ``foo_2``, ``foo_2`` -> ``foo_3``).

    Args:
        text: Text (already integer-suffixed or not).
        separator: Separator between the text and its suffix.

    Returns:
        The suffixed text, incremented.
    """
    prefix, sep, suffix = text.rpartition(separator)
    # "foo".rpartition("_")  ->  ('', '', 'foo')
    if sep == "":
        return f"{text}{separator}2"
    # "foo_".rpartition("_")  ->  ('foo', '_', '')
    if suffix == "":
        return f"{text}2"
    # "foo_1".rpartition("_")  ->  ('foo', '_', '1')
    if suffix.isdigit():
        return f"{prefix}{sep}{int(suffix) + 1}"
    # "foo_bar".rpartition("_")  ->  ('foo', '_', 'bar')
    return f"{text}{separator}2"


def to_camel_case(name: str) -> str:
    """Convert a snake_case field name to the API's camelCase key name.

    Args:
        name: Field name (e.g. "work_dir").

    Returns:
        The corresponding API key name (e.g. "workDir").
    """
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


def parse_datetime(value: Any) -> Optional[datetime]:
    """Parse a Nextflow Tower timestamp (RFC 3339) into a UTC datetime.

    Args:
        value: Timestamp string, datetime, or None.

    Returns:
        A timezone-aware UTC datetime, or None if the value is empty or cannot
        be parsed. Timestamps are only used for reporting and sorting, so an
        unexpected format should not fail a task.
    """
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            # Python 3.10's fromisoformat() does not accept the 'Z' suffix.
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            logger.warning(f"Could not parse Nextflow Tower timestamp: {value!r}")
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def api_kwargs(
    cls: type, json: dict[str, Any], overrides: Optional[dict[str, str]] = None
) -> dict[str, Any]:
    """Map an API JSON response onto the fields of a dataclass.

    Field names are matched against their camelCase equivalent unless an
    override is given. Keys that are absent from the response are left at their
    default value.

    Args:
        cls: The dataclass to build keyword arguments for.
        json: API JSON response.
        overrides: Field name to API key name, for names that don't simply
            differ by case (e.g. {"state": "status"}).

    Returns:
        Keyword arguments for the dataclass.
    """
    overrides = overrides or {}
    kwargs = {}
    for model_field in fields(cls):
        if model_field.name == "raw":
            continue
        api_key = overrides.get(model_field.name, to_camel_case(model_field.name))
        if api_key in json:
            kwargs[model_field.name] = json[api_key]
    return kwargs


@dataclass
class WorkflowStatus:
    """A workflow run's state, and what that state implies."""

    state: WorkflowState

    def __post_init__(self) -> None:
        """Coerce the state into a ``WorkflowState``.

        Raises:
            ValueError: If the state isn't a recognized workflow state. This
                fails loudly on purpose: silently mapping an unrecognized value
                onto ``UNKNOWN`` would report an in-flight run as finished.
        """
        self.state = WorkflowState(self.state)

    @property
    def is_done(self) -> bool:
        """Whether the workflow run is done, irrespective of success."""
        return self.state in TERMINAL_WORKFLOW_STATES

    @property
    def is_successful(self) -> bool:
        """Whether the workflow run succeeded."""
        return self.state == WorkflowState.SUCCEEDED

    @property
    def is_indeterminate(self) -> bool:
        return self.state == WorkflowState.UNKNOWN


@dataclass
class LaunchInfo:
    """Description of which workflow to launch on Nextflow Tower, and how.

    Attributes:
        pipeline: Pipeline to run (a repository URL or ``<org>/<repo>``).
        compute_env_id: Compute environment ID. Filled in by the hook when
            left unset.
        work_dir: S3 working directory. Defaults to the compute environment's
            own work directory when left unset.
        revision: Pipeline revision (branch, tag, or commit).
        nextflow_config: Extra Nextflow config text.
        run_name: Name for the run. Also used as the prefix the hook matches on
            when looking for previous runs of the same workflow.
        pre_run_script: Script to run before the pipeline. Defaults to the
            compute environment's own pre-run script when left unset.
        params: Pipeline parameters (Nextflow Tower's "params" YAML/JSON).
        profiles: Nextflow config profiles to apply, in order.
        user_secrets: Names of user-scoped Nextflow Tower secrets to expose.
        workspace_secrets: Names of workspace-scoped Nextflow Tower secrets to expose.
        label_ids: IDs of the labels to attach to the run.
        resume: Whether to resume a previous run.
        session_id: Session ID of the run being resumed.
        entry_name: Workflow entry point name.
    """

    pipeline: Optional[str] = None
    compute_env_id: Optional[str] = None
    work_dir: Optional[str] = None
    revision: Optional[str] = None
    nextflow_config: Optional[str] = None
    run_name: Optional[str] = None
    pre_run_script: Optional[str] = None
    params: Optional[dict[str, Any]] = None
    profiles: list[str] = field(default_factory=list)
    user_secrets: list[str] = field(default_factory=list)
    workspace_secrets: list[str] = field(default_factory=list)
    label_ids: list[int] = field(default_factory=list)
    resume: bool = False
    session_id: Optional[str] = None
    entry_name: Optional[str] = ""

    def __post_init__(self) -> None:
        """Validate that resume and session_id are in sync.

        Raises:
            ValueError: If resume is enabled without a session ID.
        """
        if self.resume and self.session_id is None:
            raise ValueError("Resume can only be enabled with a session ID.")

    def require(self, attr: str) -> Any:
        """Retrieve an attribute value that must be set by now.

        Args:
            attr: Attribute name.

        Raises:
            ValueError: If the attribute is unset.

        Returns:
            The attribute value.
        """
        value = getattr(self, attr, None)
        if value is None:
            raise ValueError(f"LaunchInfo attribute '{attr}' must be set by now.")
        return value

    def fill_in(self, attr: str, value: Any) -> None:
        """Set an attribute if it is currently missing or falsy.

        Args:
            attr: Attribute name.
            value: Value to fall back on.
        """
        if not getattr(self, attr, None):
            setattr(self, attr, value)

    def add_in(self, attr: str, values: Iterable[Any]) -> None:
        """Append values to one of the list attributes.

        Args:
            attr: Attribute name.
            values: Values to add.

        Raises:
            ValueError: If the attribute isn't a list.
        """
        current_values = getattr(self, attr)
        if not isinstance(current_values, list):
            raise ValueError(
                f"Attribute '{attr}' is not a list and cannot be extended."
            )
        setattr(self, attr, dedup(current_values + list(values)))

    def to_json(self) -> dict[str, Any]:
        """Generate the request payload for Nextflow Tower's workflow launch endpoint.

        Returns:
            JSON representation of this launch specification.
        """
        launch = {
            "computeEnvId": self.require("compute_env_id"),
            "configProfiles": dedup(self.profiles),
            "configText": self.nextflow_config,
            "dateCreated": None,
            "entryName": self.require("entry_name"),
            "headJobCpus": None,
            "headJobMemoryMb": None,
            "id": None,
            "labelIds": dedup(self.label_ids),
            "mainScript": None,
            "optimizationId": None,
            "paramsText": json_module.dumps(self.params) if self.params else "",
            "pipeline": self.require("pipeline"),
            "postRunScript": None,
            "preRunScript": self.pre_run_script,
            "pullLatest": False,
            "revision": self.revision,
            "runName": self.run_name,
            "schemaName": None,
            "stubRun": False,
            "towerConfig": None,
            "userSecrets": dedup(self.user_secrets),
            "workDir": self.require("work_dir"),
            "workspaceSecrets": dedup(self.workspace_secrets),
        }
        if self.resume:
            launch["resume"] = self.resume
            launch["sessionId"] = self.require("session_id")
        return {"launch": launch}


@dataclass
class Workflow:
    """Details about a Nextflow Tower workflow run."""

    id: str
    run_name: Optional[str] = None
    state: WorkflowState = WorkflowState.UNKNOWN
    session_id: Optional[str] = None
    username: Optional[str] = None
    project_name: Optional[str] = None
    work_dir: Optional[str] = None
    params: Optional[dict[str, Any]] = None
    commit_id: Optional[str] = None
    submit: Optional[datetime] = None
    complete: Optional[datetime] = None
    raw: Optional[dict[str, Any]] = field(default=None, repr=False, compare=False)

    key_overrides = {"state": "status", "username": "userName"}

    def __post_init__(self) -> None:
        """Coerce the run state into a ``WorkflowState``."""
        self.state = WorkflowState(self.state)

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> "Workflow":
        """Create an instance from an API JSON response.

        Args:
            json: API JSON response for a single workflow run.

        Returns:
            Workflow instance.
        """
        kwargs = api_kwargs(cls, json, cls.key_overrides)
        kwargs["submit"] = parse_datetime(json.get("submit"))
        kwargs["complete"] = parse_datetime(json.get("complete"))
        return cls(raw=json, **kwargs)

    def __repr__(self) -> str:
        """String representation of a workflow run."""
        return f"Workflow(run_name={self.run_name}, id={self.id}, state={self.state})"

    @property
    def status(self) -> WorkflowStatus:
        """Workflow run status."""
        return WorkflowStatus(self.state)


@dataclass
class WorkflowTask:
    """Details about a single task within a Nextflow Tower workflow run."""

    task_id: Optional[int] = None
    status: Optional[str] = None
    name: Optional[str] = None
    process: Optional[str] = None
    tag: Optional[str] = None
    attempt: Optional[int] = None
    exit_status: Optional[int] = None
    duration: Optional[int] = None
    cpus: Optional[int] = None
    memory: Optional[int] = None
    disk: Optional[int] = None
    container: Optional[str] = None
    executor: Optional[str] = None
    queue: Optional[str] = None
    machine_type: Optional[str] = None
    price_model: Optional[str] = None
    cost: Optional[float] = None
    error_action: Optional[str] = None
    native_id: Optional[str] = None
    work_dir: Optional[str] = None
    raw: Optional[dict[str, Any]] = field(default=None, repr=False, compare=False)

    # Nextflow Tower spells this one "workdir" (all lowercase) on task payloads.
    key_overrides = {"work_dir": "workdir"}

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> "WorkflowTask":
        """Create an instance from an API JSON response.

        Args:
            json: API JSON response for a single task.

        Returns:
            WorkflowTask instance.
        """
        return cls(raw=json, **api_kwargs(cls, json, cls.key_overrides))


@dataclass
class Label:
    """A Nextflow Tower workflow run label."""

    id: int
    name: Optional[str] = None
    value: Optional[str] = None
    resource: bool = False

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> "Label":
        """Create an instance from an API JSON response.

        Args:
            json: API JSON response for a single label.

        Returns:
            Label instance.
        """
        return cls(**api_kwargs(cls, json))


@dataclass
class ComputeEnv:
    """A Nextflow Tower compute environment.

    Both the summary form (from listing compute environments) and the detailed
    form (from fetching one) map onto this class.
    """

    id: str
    name: Optional[str] = None
    status: Optional[str] = None
    work_dir: Optional[str] = None
    pre_run_script: Optional[str] = None
    date_created: Optional[datetime] = None
    labels: list[Label] = field(default_factory=list)
    raw: Optional[dict[str, Any]] = field(default=None, repr=False, compare=False)

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> "ComputeEnv":
        """Create an instance from an API JSON response.

        Args:
            json: API JSON response for a single compute environment.

        Returns:
            ComputeEnv instance.
        """
        kwargs = api_kwargs(cls, json)
        # Listings carry workDir at the top level; the detail endpoint nests the
        # work directory and the pre-run script under "config".
        config = json.get("config") or {}
        kwargs["work_dir"] = config.get("workDir", json.get("workDir"))
        kwargs["pre_run_script"] = config.get("preRunScript")
        kwargs["date_created"] = parse_datetime(json.get("dateCreated"))
        kwargs["labels"] = [
            Label.from_json(label) for label in json.get("labels") or []
        ]
        return cls(raw=json, **kwargs)


@dataclass
class Workspace:
    """A Nextflow Tower workspace, and the organization that owns it."""

    id: int
    name: str
    org_name: str

    @property
    def full_name(self) -> str:
        """Fully-qualified workspace name (prefixed with the organization)."""
        return f"{self.org_name}/{self.name}".lower()

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> "Workspace":
        """Create an instance from an API JSON response.

        Args:
            json: An item from the user's list of orgs and workspaces.

        Returns:
            Workspace instance.
        """
        return cls(
            id=json["workspaceId"],
            name=json["workspaceName"],
            org_name=json["orgName"],
        )


class NextflowTowerClient:
    """Thin authenticated HTTP client for Nextflow Tower's REST API.

    Attributes:
        api_endpoint: API base endpoint
            (e.g. "https://tower.sagebionetworks.org/api").
        auth_token: Personal access token for that endpoint.
    """

    def __init__(self, api_endpoint: str, auth_token: str) -> None:
        self.api_endpoint = api_endpoint.rstrip("/")
        self.auth_token = auth_token

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """Make an authenticated HTTP request.

        Args:
            method: An HTTP method (GET, PUT, POST, or DELETE).
            path: API path, with any parameters already filled in.
            **kwargs: Additional arguments passed to ``requests.request()``.

        Returns:
            The raw response, to allow for special handling.
        """
        url = f"{self.api_endpoint}/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        headers.update(kwargs.pop("headers", None) or {})
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
        return requests.request(method, url, headers=headers, **kwargs)

    def request_json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make an authenticated request and parse the JSON response.

        Args:
            method: An HTTP method (GET, PUT, POST, or DELETE).
            path: API path, with any parameters already filled in.
            **kwargs: Additional arguments passed to ``requests.request()``.

        Raises:
            HTTPError: If the request failed. The response body is included in
                the message, since Nextflow Tower explains failures there.

        Returns:
            The deserialized JSON response.
        """
        response = self.request(method, path, **kwargs)
        try:
            response.raise_for_status()
        except HTTPError as error:
            raise HTTPError(f"{error} - {response.text}") from error
        return response.json()

    def get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Send an authenticated GET request and parse the JSON response.

        Args:
            path: API path, with any parameters already filled in.
            **kwargs: Additional arguments passed to ``requests.request()``.

        Returns:
            The deserialized JSON response.
        """
        return self.request_json("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Send an authenticated POST request and parse the JSON response.

        Args:
            path: API path, with any parameters already filled in.
            **kwargs: Additional arguments passed to ``requests.request()``.

        Returns:
            The deserialized JSON response.
        """
        return self.request_json("POST", path, **kwargs)

    def unwrap(self, json: dict[str, Any], key: str) -> Any:
        """Pull a top-level key out of a JSON response.

        Args:
            json: JSON response.
            key: Key the value is nested under.

        Raises:
            HTTPError: If the key isn't in the response.

        Returns:
            The nested value.
        """
        if key not in json:
            raise HTTPError(f"Expecting '{key}' key in JSON response ({json}).")
        return json[key]

    def get_items(
        self, path: str, items_key: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """GET a list of items, following pagination when the endpoint pages.

        Paged Nextflow Tower endpoints report a ``totalSize`` (or ``total``) alongside
        the current page of items; unpaged ones return everything at once.

        Args:
            path: API path, with any parameters already filled in.
            items_key: Key the list of items is nested under.
            params: URL query parameters.

        Returns:
            The items from every page.
        """
        json = self.get(path, params=params)
        items = list(self.unwrap(json, items_key))

        total_size = json.get("totalSize", json.get("total"))
        if total_size is None:
            return items

        page_params = dict(params or {}, max=PAGE_SIZE)
        while len(items) < total_size:
            page_params["offset"] = len(items)
            page = self.unwrap(self.get(path, params=page_params), items_key)
            if not page:
                logger.warning(
                    f"Expected {total_size} items from '{path}' but pagination "
                    f"stopped after {len(items)}."
                )
                break
            items.extend(page)
        return items


@dataclass
class NextflowTowerConfig:
    """Resolved credentials and target workspace for a Nextflow Tower connection."""

    api_endpoint: str
    auth_token: str
    workspace: Optional[str] = None


class NextflowTowerHook:
    """Airflow hook for Nextflow Tower.

    Resolves credentials from an Airflow connection (host/schema = API
    endpoint, password = access token, ``extra.workspace`` = fully-qualified
    workspace name), with ``TOWER_*`` environment variable fallbacks for local
    runs. See this module's docstring for the connection layout.

    Class Variables:
        launch_label: Label attached to every run launched through this hook,
            and the label used to find previous runs. Keep the "orca" name:
            runs launched by py-orca carry it, and changing it would hide those
            runs from ``launch_workflow()``'s resume/deduplication.
        default_api_endpoint: API endpoint used when neither the connection nor
            the environment specifies one.
    """

    launch_label = "launched-by-orca"
    default_api_endpoint = "https://tower.sagebionetworks.org/api"

    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id
        self._config: Optional[NextflowTowerConfig] = None
        self._client: Optional[NextflowTowerClient] = None
        self._workspace_id: Optional[int] = None

    @property
    def config(self) -> NextflowTowerConfig:
        """Resolved Nextflow Tower credentials for this connection."""
        if self._config is None:
            self._config = self._resolve_config()
        return self._config

    @property
    def client(self) -> NextflowTowerClient:
        """Authenticated Nextflow Tower API client."""
        if self._client is None:
            self._client = NextflowTowerClient(
                self.config.api_endpoint, self.config.auth_token
            )
        return self._client

    @property
    def workspace(self) -> str:
        """Fully-qualified name of the active workspace (lowercased).

        Raises:
            EnvironmentError: If no workspace was configured.
        """
        if not self.config.workspace:
            raise EnvironmentError(
                f"Connection '{self.conn_id}' does not specify a workspace. Set "
                "'workspace' in the connection's extras (or the TOWER_WORKSPACE "
                "env var) to '<organization-name>/<workspace-name>'."
            )
        return self.config.workspace.lower()

    @property
    def workspace_id(self) -> int:
        """Numeric ID of the active workspace."""
        if self._workspace_id is None:
            self._workspace_id = self._resolve_workspace_id()
        return self._workspace_id

    def get_latest_compute_env(self, name_filter: Optional[str] = None) -> str:
        """Get the most recently created available compute environment.

        Args:
            name_filter: Substring that a compute environment's name must
                contain (e.g. "ondemand"). Defaults to no filtering.

        Raises:
            ValueError: If no available compute environment matches.

        Returns:
            Compute environment ID.
        """
        envs = self.list_compute_envs(status="AVAILABLE")
        if name_filter:
            envs = [env for env in envs if name_filter in (env.name or "")]
        if not envs:
            raise ValueError(
                f"No available compute environments in workspace "
                f"'{self.workspace}' matching filter: {name_filter!r}"
            )
        if len(envs) == 1:
            return envs[0].id

        # Listings omit dateCreated, so the details are needed to pick the latest.
        detailed = [self.get_compute_env(env.id) for env in envs]
        return max(detailed, key=lambda env: env.date_created or EPOCH).id

    def create_label(self, name: str) -> int:
        """Create a (non-resource) workflow label, or reuse the existing one.

        Args:
            name: Label name.

        Returns:
            Label ID.
        """
        for label in self.list_labels():
            if not label.resource and label.name == name:
                return label.id
        json = self.client.post(
            "/labels",
            params=self._params(),
            json={"name": name, "resource": False},
        )
        return Label.from_json(json).id

    def launch_workflow(
        self,
        launch_info: LaunchInfo,
        compute_env_filter: Optional[str] = None,
        ignore_previous_runs: bool = False,
    ) -> str:
        """Launch a workflow on the latest matching compute environment.

        Unless ``ignore_previous_runs`` is set, previous runs of the same
        pipeline and run name are taken into account: an ongoing, succeeded, or
        unknown-state run is returned as-is instead of launching a duplicate,
        and a failed or cancelled run is resumed under an incremented run name.

        Args:
            launch_info: Workflow launch information.
            compute_env_filter: Substring that the compute environment's name
                must contain. Defaults to no filtering.
            ignore_previous_runs: Whether to launch unconditionally. Note that
                enabling this can produce duplicate workflow runs.

        Raises:
            ValueError: If ``run_name`` or ``pipeline`` is unset.

        Returns:
            Workflow run ID.
        """
        if launch_info.pipeline is None or launch_info.run_name is None:
            raise ValueError(
                "LaunchInfo 'run_name' and 'pipeline' attributes must be set."
            )

        if not ignore_previous_runs:
            latest_run = self.get_latest_previous_workflow(launch_info)
            if latest_run:
                state = latest_run.status.state
                run_repr = f"{latest_run.run_name} (id='{latest_run.id}', {state=})"
                if not latest_run.status.is_done:
                    logger.info(f"Found an ongoing previous run: {run_repr}")
                    return latest_run.id
                if state in {WorkflowState.SUCCEEDED, WorkflowState.UNKNOWN}:
                    logger.info(f"Found a previous run: {run_repr}")
                    return latest_run.id
                launch_info.fill_in("resume", True)
                launch_info.fill_in("session_id", latest_run.session_id)
                launch_info.run_name = increment_suffix(latest_run.run_name or "")
                logger.info(f"Relaunching from a previous run: {run_repr}")

        # Inherit the compute environment's defaults and its resource labels.
        compute_env = self.get_compute_env(
            self.get_latest_compute_env(compute_env_filter)
        )
        label_ids = [label.id for label in compute_env.labels]

        # Ensure that every run is labeled, so it can be found again later.
        label_ids.append(self.create_label(self.launch_label))

        launch_info.fill_in("compute_env_id", compute_env.id)
        launch_info.fill_in("work_dir", compute_env.work_dir)
        launch_info.fill_in("pre_run_script", compute_env.pre_run_script)
        launch_info.add_in("label_ids", label_ids)

        json = self.client.post(
            "/workflow/launch", params=self._params(), json=launch_info.to_json()
        )
        workflow_id = self.client.unwrap(json, "workflowId")
        logger.info(
            f"Launched a new workflow run: {launch_info.run_name} ({workflow_id})"
        )
        return workflow_id

    def get_workflow(self, workflow_id: str) -> Workflow:
        """Retrieve details about a workflow run.

        Args:
            workflow_id: Workflow run ID.

        Returns:
            Workflow instance.
        """
        json = self.client.get(f"/workflow/{workflow_id}", params=self._params())
        return Workflow.from_json(self.client.unwrap(json, "workflow"))

    def list_workflows(self, search_filter: str = "") -> list[Workflow]:
        """List workflow runs launched by this hook that match a search filter.

        Args:
            search_filter: A Nextflow Tower search query, as you would compose it in
                the runs search bar. Defaults to no additional filtering.

        Returns:
            List of workflow instances.
        """
        search_filter = f"{search_filter} label:{self.launch_label}".strip()
        items = self.client.get_items(
            "/workflow", "workflows", params=self._params(search=search_filter)
        )
        return [Workflow.from_json(item["workflow"]) for item in items]

    def list_previous_workflows(self, launch_info: LaunchInfo) -> list[Workflow]:
        """Retrieve previously launched runs of the same workflow.

        Args:
            launch_info: Workflow launch information.

        Returns:
            List of previously launched workflows for this pipeline and run name
            prefix.
        """
        previous_workflows = []
        # TODO: can we use search filter parameter of `list_workflows` to pass a runName: filter?
        for workflow in self.list_workflows():
            if workflow.project_name != launch_info.pipeline:
                continue
            prefix = launch_info.run_name
            if prefix and not (workflow.run_name or "").startswith(prefix):
                continue
            previous_workflows.append(workflow)
        return previous_workflows

    def get_latest_previous_workflow(
        self, launch_info: LaunchInfo
    ) -> Optional[Workflow]:
        """Retrieve the latest run among previously launched workflows.

        Args:
            launch_info: Workflow launch information.

        Raises:
            ValueError: If more than one previous run is still ongoing.

        Returns:
            The ongoing run if there is one, otherwise the most recently
            submitted run, or None if there are no previous runs.
        """
        previous_runs = self.list_previous_workflows(launch_info)
        if not previous_runs:
            return None

        ongoing_runs = [run for run in previous_runs if not run.status.is_done]
        if len(ongoing_runs) > 1:
            raise ValueError(f"Multiple ongoing workflow runs: {ongoing_runs}")
        if len(ongoing_runs) == 1:
            return ongoing_runs[0]

        return max(previous_runs, key=lambda run: run.submit or EPOCH)

    def get_workflow_tasks(self, workflow_id: str) -> list[WorkflowTask]:
        """Retrieve the details of a workflow run's tasks.

        Args:
            workflow_id: Workflow run ID.

        Returns:
            List of task details.
        """
        items = self.client.get_items(
            f"/workflow/{workflow_id}/tasks", "tasks", params=self._params()
        )
        return [WorkflowTask.from_json(item["task"]) for item in items]

    def get_task_logs(self, workflow_id: str, task_id: int) -> str:
        """Retrieve the execution logs for a given workflow task.

        Args:
            workflow_id: Workflow run ID.
            task_id: Task ID.

        Returns:
            Task logs.
        """
        json = self.client.get(
            f"/workflow/{workflow_id}/log/{task_id}", params=self._params()
        )
        log = self.client.unwrap(json, "log")
        return "\n".join(log["entries"])

    def list_compute_envs(self, status: Optional[str] = None) -> list[ComputeEnv]:
        """List the workspace's compute environments.

        Args:
            status: Compute environment status to filter on
                (e.g. "AVAILABLE"). Defaults to no filtering.

        Returns:
            List of compute environment summaries.
        """
        items = self.client.get_items(
            "/compute-envs", "computeEnvs", params=self._params(status=status)
        )
        return [ComputeEnv.from_json(item) for item in items]

    def get_compute_env(self, compute_env_id: str) -> ComputeEnv:
        """Retrieve the details of a single compute environment.

        Args:
            compute_env_id: Compute environment ID.

        Returns:
            Compute environment instance, including its labels.
        """
        json = self.client.get(
            f"/compute-envs/{compute_env_id}",
            params=self._params(attributes="labels"),
        )
        return ComputeEnv.from_json(self.client.unwrap(json, "computeEnv"))

    def list_labels(self) -> list[Label]:
        """List the workspace's labels.

        Returns:
            List of available labels.
        """
        items = self.client.get_items("/labels", "labels", params=self._params())
        return [Label.from_json(item) for item in items]

    def _params(self, **kwargs: Any) -> dict[str, Any]:
        """Build URL query parameters scoped to the active workspace.

        Args:
            **kwargs: Additional query parameters, included only when they are
                not None.

        Returns:
            URL query parameters.
        """
        params: dict[str, Any] = {"workspaceId": self.workspace_id}
        params.update({key: value for key, value in kwargs.items() if value is not None})
        return params

    def _resolve_workspace_id(self) -> int:
        """Look up the numeric ID of the configured workspace.

        Raises:
            ValueError: If the workspace isn't available to the token's user.

        Returns:
            Workspace ID.
        """
        user = self.client.unwrap(self.client.get("/user-info"), "user")
        json = self.client.get(f"/user/{user['id']}/workspaces")
        workspaces = [
            Workspace.from_json(item)
            for item in self.client.unwrap(json, "orgsAndWorkspaces")
            # Organizations show up in this listing with a null workspace ID.
            if item.get("workspaceId")
        ]
        for workspace in workspaces:
            if workspace.full_name == self.workspace:
                return workspace.id
        available = sorted(workspace.full_name for workspace in workspaces)
        raise ValueError(
            f"Workspace '{self.workspace}' is not available to user "
            f"'{user.get('userName')}'. Available workspaces: {available}"
        )

    def _resolve_config(self) -> NextflowTowerConfig:
        """Resolve credentials from the Airflow connection, then the environment.

        Raises:
            EnvironmentError: If no access token could be resolved.

        Returns:
            The resolved configuration.
        """
        api_endpoint = auth_token = workspace = None

        connection = self._get_connection()
        if connection is not None:
            api_endpoint, auth_token, workspace = self._parse_connection(connection)

        api_endpoint = (
            api_endpoint
            or os.environ.get("TOWER_API_ENDPOINT")
            or self.default_api_endpoint
        )
        auth_token = auth_token or os.environ.get("TOWER_ACCESS_TOKEN")
        workspace = workspace or os.environ.get("TOWER_WORKSPACE")

        if not auth_token:
            raise EnvironmentError(
                f"Could not resolve an access token from the Airflow connection "
                f"'{self.conn_id}' and the TOWER_ACCESS_TOKEN env var is not set."
            )
        if workspace:
            self._validate_workspace(workspace)

        return NextflowTowerConfig(api_endpoint.rstrip("/"), auth_token, workspace)

    def _get_connection(self) -> Optional[Any]:
        """Retrieve the Airflow connection, if one is resolvable.

        Returns:
            The Airflow connection, or None when Airflow or the connection
            isn't available (e.g. a local run without a metadata database).
        """
        try:
            from airflow.hooks.base import BaseHook

            return BaseHook.get_connection(self.conn_id)
        except AirflowNotFoundException:
            logger.info(
                f"Could not resolve Airflow connection '{self.conn_id}'; "
                "falling back to TOWER_* environment variables."
            )
            return None

    @staticmethod
    def _parse_connection(
        connection: Any,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse an Airflow connection into Nextflow Tower configuration values.

        Args:
            connection: An Airflow connection object.

        Returns:
            The API endpoint, access token, and workspace name.
        """
        api_endpoint = None
        if connection.host:
            schema = connection.schema or ""
            api_endpoint = f"https://{connection.host}/{schema}".rstrip("/")
        return (
            api_endpoint,
            connection.password,
            connection.extra_dejson.get("workspace"),
        )

    @staticmethod
    def _validate_workspace(workspace: str) -> None:
        """Validate a fully-qualified workspace name.

        Args:
            workspace: Workspace name to validate.

        Raises:
            ValueError: If the name isn't '<organization-name>/<workspace-name>'.
        """
        if not re.fullmatch(r"[^/]+/[^/]+", workspace):
            raise ValueError(
                f"Workspace ('{workspace}') should be structured as "
                "'<organization-name>/<workspace-name>'."
            )

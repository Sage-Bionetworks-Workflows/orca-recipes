"""Unit/structural tests for all DAGs.

These tests import DAGs in a separate subprocess to mimic Airflow's import
environment as closely as possible. Running `DagBag` directly in the pytest
process inherits pytest's `sys.path`, which includes the repository root and
can mask import issues that would fail when Airflow parses DAGs.

For example, this incorrect import succeeds under `python -m pytest` because
the repository root is on `sys.path`:

    from dags.src.utils ...

However, Airflow imports DAGs relative to the configured DAG folder, so the
correct import is:

    from src.utils ...

Using a fresh subprocess allows us to control PYTHONPATH and the working
directory so DAG imports behave the same way they do when parsed by Airflow.
"""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Dict

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_FOLDER = REPO_ROOT / "dags"
DAG_FILES = sorted(DAG_FOLDER.rglob("*.py"))

# DAGs that read Airflow Variables/Connections at parse time and therefore
# cannot be import validated without those secrets present in the environment.
KNOWN_PARSE_TIME_DEPENDENCIES: set[str] = {
    "synapse-dataset-to-croissant.py",
    "synapse_dataset_to_croissant_minimal.py",
}

def _parse_dags_like_airflow() -> Dict[str, dict]:
    """Parse every DAG in a subprocess with only the dags/ folder on sys.path.

    Returns:
        Dict[str, dict]: A dictionary containing import errors and DAG information
    """
    # Marker prefixed onto the subprocess's JSON result line so we can pick it
    # out of stdout even if Airflow logs other noise.
    result_marker = "__DAGBAG_RESULT__"

    # Script run in the subprocess: build a DagBag over the whole folder with an
    # Airflow-style sys.path and emit import errors + discovered dag ids as one
    # JSON line
    dagbag_parse_script = textwrap.dedent(
        f"""
        import json, sys
        from airflow.models import DagBag

        bag = DagBag(dag_folder=sys.argv[1], include_examples=False)
        print({result_marker!r} + json.dumps({{
            "import_errors": {{str(k): str(v) for k, v in bag.import_errors.items()}},
            "dags": {{
                dag_id: {{
                    "num_tasks": len(dag.tasks),
                    "tags": sorted(dag.tags or []),
                }}
                for dag_id, dag in bag.dags.items()
            }},
        }}))
        """
    )

    env = dict(os.environ)
   # Mimic Airflow's runtime by exposing only the DAGs folder on PYTHONPATH,
    # preventing accidental imports from the repository root
    env["PYTHONPATH"] = str(DAG_FOLDER)
    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    result = subprocess.run(
        [sys.executable, "-c", dagbag_parse_script, str(DAG_FOLDER)],
        capture_output=True,
        text=True,
        # Set the working directory to dags/ so Python's implicit -c path entry
        # does not expose the repository root
        cwd=str(DAG_FOLDER),
        env=env,
    )

    marker_lines = [
        line[len(result_marker):]
        for line in result.stdout.splitlines()
        if line.startswith(result_marker)
    ]
    assert marker_lines, (
        "DAG parsing subprocess produced no result.\n"
        f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    )
    return json.loads(marker_lines[-1])


# Parsed once at collection time, keys of import_errors are absolute file paths.
_PARSE_RESULT = _parse_dags_like_airflow()
_IMPORT_ERRORS = {
    str(Path(path).resolve().relative_to(DAG_FOLDER.resolve())): error
    for path, error in _PARSE_RESULT["import_errors"].items()
}

@pytest.mark.parametrize(
    "dag_file",
    DAG_FILES,
    ids=lambda path: str(path.relative_to(DAG_FOLDER)),
)
def test_all_dag_imports_successfully(dag_file):
    """Tests that each DAG file imports cleanly under Airflow's runtime sys.path"""
    relative_path = str(dag_file.relative_to(DAG_FOLDER))

    if dag_file.name in KNOWN_PARSE_TIME_DEPENDENCIES:
        pytest.skip(
            f"{relative_path} is an allowlisted parse-time-dependent DAG"
        )

    error = _IMPORT_ERRORS.get(relative_path)

    assert error is None, (
        f"Import error in {relative_path}:\n{error}"
    )


def test_no_stale_parse_time_dependency_allowlist():
    """Allowlisted DAGs must still actually fail, or the entry is stale"""
    stale = KNOWN_PARSE_TIME_DEPENDENCIES - set(_IMPORT_ERRORS)
    assert not stale, (
        "These DAGs are allowlisted for parse-time failures but no longer fail. "
        f"Remove them from KNOWN_PARSE_TIME_DEPENDENCIES: {stale}"
    )


@pytest.mark.parametrize("dag_id", sorted(_PARSE_RESULT["dags"]))
def test_all_dag_has_tasks(dag_id):
    """Each DAG defines at least one task."""
    assert _PARSE_RESULT["dags"][dag_id]["num_tasks"] >= 1, (
        f"DAG '{dag_id}' has no tasks"
    )

@pytest.mark.skip(reason="Skipping tag tests for now until we establish a tagging convention")
@pytest.mark.parametrize("dag_id", sorted(_PARSE_RESULT["dags"]))
def test_all_dag_has_tags(dag_id):
    """Each DAG declares at least one tag."""
    assert _PARSE_RESULT["dags"][dag_id]["tags"], f"DAG '{dag_id}' has no tags"

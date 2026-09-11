"""Shared sys.path setup for standalone integration scripts in this directory.

Import this before any `dags.*`/`src.*` import, e.g.:
    import _bootstrap  # noqa: F401

dags/ must be on the path too, since dags/src/nextflow_tower_hook.py imports
`from src.utils import ...` (src -> dags/src).
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "dags"))

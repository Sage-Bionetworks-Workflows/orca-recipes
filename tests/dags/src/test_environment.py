"""Tests for dags/src/environment.py."""

import importlib

import pytest

from dags.src import environment


def test_environment_defaults_to_dev(monkeypatch):
    monkeypatch.delenv("AIRFLOW__ORCA__ENVIRONMENT", raising=False)

    assert importlib.reload(environment).ENVIRONMENT == "dev"


@pytest.mark.parametrize(
    "env, expected",
    [("dev", "d"), ("prod", "p"), ("staging", "s"), ("unknown", "d")],
)
def test_by_environment_picks_value_for_environment(monkeypatch, env, expected):
    monkeypatch.setattr(environment, "ENVIRONMENT", env)

    assert environment.by_environment(dev="d", prod="p", staging="s") == expected

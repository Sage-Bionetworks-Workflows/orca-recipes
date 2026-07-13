#!/usr/bin/env bash
set -euo pipefail

echo "Installing dependencies..."
pip install -r requirements-local-airflow.txt

echo "Installing synapseclient (kept separate to avoid a urllib3 conflict) and setuptools..."
python -m pip install \
  "synapseclient[pandas]~=4.8" \
  "setuptools<82"

echo "Verifying environment..."
python -m pip check

#!/usr/bin/env bash
set -euo pipefail

echo "Installing dependencies..."
python -m pip install -r requirements-local-airflow.txt

echo "Verifying environment..."
python -m pip check

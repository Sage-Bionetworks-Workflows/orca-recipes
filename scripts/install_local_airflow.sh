#!/usr/bin/env bash
set -euo pipefail

airflow_version=$(bash "$(dirname "$0")/mwaa_versions.sh" | sed -n 's/^airflow=//p')

echo "Installing Airflow ${airflow_version} and dependencies..."
python -m pip install "apache-airflow[amazon,celery,snowflake]==${airflow_version}" \
  -r requirements-local-airflow.txt

echo "Verifying environment..."
python -m pip check

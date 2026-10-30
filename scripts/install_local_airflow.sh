#!/usr/bin/env bash
set -euo pipefail

versions=$(bash "$(dirname "$0")/mwaa_versions.sh")
airflow_version=$(sed -n 's/^airflow=//p' <<<"$versions")
python_version=$(sed -n 's/^python=//p' <<<"$versions")

if [[ "$(python -c 'import sys; print("%d.%d" % sys.version_info[:2])')" != "$python_version" ]]; then
  echo "MWAA runs Python ${python_version}, but python is $(python --version). Activate a Python ${python_version} virtual environment first." >&2
  exit 1
fi

echo "Installing Airflow ${airflow_version} and dependencies..."
python -m pip install "apache-airflow[amazon,celery,snowflake]==${airflow_version}" \
  -r requirements-local-airflow.txt

echo "Verifying environment..."
python -m pip check
if [[ "$(airflow version 2>/dev/null)" != "$airflow_version" ]]; then
  echo "airflow on PATH ($(command -v airflow)) is not Airflow ${airflow_version}. Activate the virtual environment you installed into." >&2
  exit 1
fi
echo "Python ${python_version} and Airflow ${airflow_version} are ready: $(command -v airflow)"

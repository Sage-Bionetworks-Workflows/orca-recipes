#!/usr/bin/env bash
# Prints the MWAA Airflow and Python versions as airflow=<version> and python=<version>.
# They come from the Source: line of mwaa/constraints.txt:
# .../constraints-<airflow>/constraints-<python>.txt
set -euo pipefail

versions=$(sed -nE 's|^# Source: .*/constraints-([0-9.]+)/constraints-([0-9.]+)\.txt$|airflow=\1\npython=\2|p' \
  "$(dirname "$0")/../mwaa/constraints.txt")
if [ -z "$versions" ]; then
  echo "mwaa/constraints.txt has no '# Source: .../constraints-<airflow>/constraints-<python>.txt' line" >&2
  exit 1
fi
echo "$versions"

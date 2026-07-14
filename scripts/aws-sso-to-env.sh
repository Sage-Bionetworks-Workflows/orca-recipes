#!/usr/bin/env bash
# Log in via AWS SSO and export short-lived credentials into .env for docker compose.
# Usage: bash scripts/aws-sso-to-env.sh <your-sso-profile>
# Credentials are short-lived; re-run this script to refresh them.
set -euo pipefail

PROFILE="${1:-${AWS_PROFILE:-}}"
if [[ -z "${PROFILE}" ]]; then
  echo "Usage: bash scripts/aws-sso-to-env.sh <your-sso-profile>" >&2
  exit 1
fi

ENV_FILE="$(git rev-parse --show-toplevel 2>/dev/null || echo .)/.env"

aws sso login --profile "${PROFILE}"

# Strip any previously exported AWS keys, then append the current ones.
if [[ -f "${ENV_FILE}" ]]; then
  grep -vE '^(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)=' "${ENV_FILE}" > "${ENV_FILE}.tmp"
  mv "${ENV_FILE}.tmp" "${ENV_FILE}"
fi
aws configure export-credentials --profile "${PROFILE}" --format env-no-export >> "${ENV_FILE}"

echo "Wrote short-lived AWS credentials for profile '${PROFILE}' to ${ENV_FILE}"

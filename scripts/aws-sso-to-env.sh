#!/usr/bin/env bash
# Log in via AWS SSO and write short-lived credentials into .env for docker compose.
# Usage: bash scripts/aws-sso-to-env.sh <your-sso-profile>
# Only the AWS credential lines are touched; all other .env content is left intact.
# Credentials are short-lived; re-run this script to refresh them.
set -euo pipefail

PROFILE="${1:-${AWS_PROFILE:-}}"
if [[ -z "${PROFILE}" ]]; then
  echo "Usage: bash scripts/aws-sso-to-env.sh <your-sso-profile>" >&2
  exit 1
fi

ENV_FILE="$(git rev-parse --show-toplevel 2>/dev/null || echo .)/.env"

aws sso login --profile "${PROFILE}"

# Fetch the credentials first, so a failure here never mutates the user's .env.
CREDS="$(aws configure export-credentials --profile "${PROFILE}" --format env-no-export)"

touch "${ENV_FILE}"

# Upsert each credential line in place: replace an existing assignment, or append
# it if absent. Every other line in .env is preserved untouched.
while IFS= read -r line; do
  key="${line%%=*}"
  [[ -z "${key}" ]] && continue
  if grep -qE "^${key}=" "${ENV_FILE}"; then
    # Use a non-/ delimiter since credential values contain '/'.
    sed -i "s|^${key}=.*|${line}|" "${ENV_FILE}"
  else
    printf '%s\n' "${line}" >> "${ENV_FILE}"
  fi
done <<< "${CREDS}"

echo "Updated AWS credentials for profile '${PROFILE}' in ${ENV_FILE}"

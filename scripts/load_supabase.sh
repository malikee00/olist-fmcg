#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Wrapper: load_supabase.sh -> publish_postgres.sh"

: "${PROJECT_ROOT:?PROJECT_ROOT not set}"

export DB_HOST="${AZURE_DB_HOST:-${SUPABASE_DB_HOST:-}}"
export DB_PORT="${AZURE_DB_PORT:-${SUPABASE_DB_PORT:-5432}}"
export DB_NAME="${AZURE_DB_NAME:-${SUPABASE_DB_NAME:-postgres}}"
export DB_USER="${AZURE_DB_USER:-${SUPABASE_DB_USER:-}}"
export DB_PASSWORD="${AZURE_DB_PASSWORD:-${SUPABASE_DB_PASSWORD:-}}"
export DB_SSLMODE="${AZURE_DB_SSLMODE:-${SUPABASE_DB_SSLMODE:-require}}"
export DB_SCHEMA="${AZURE_DB_SCHEMA:-analytics}"

: "${DB_HOST:?DB_HOST not set}"
: "${DB_USER:?DB_USER not set}"
: "${DB_PASSWORD:?DB_PASSWORD not set}"
: "${DB_PORT:?DB_PORT not set}"
: "${DB_NAME:?DB_NAME not set}"

exec "$(dirname "$0")/publish_postgres.sh"

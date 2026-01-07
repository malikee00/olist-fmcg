#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Wrapper: load_supabase.sh -> publish_postgres.sh"

: "${PROJECT_ROOT:?PROJECT_ROOT not set}"
: "${DB_HOST:?DB_HOST not set}"
: "${DB_USER:?DB_USER not set}"
: "${DB_PASSWORD:?DB_PASSWORD not set}"
: "${DB_PORT:?DB_PORT not set}"
: "${DB_NAME:?DB_NAME not set}"

exec "$(dirname "$0")/publish_postgres.sh"

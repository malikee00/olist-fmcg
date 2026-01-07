#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Wrapper: load_supabase.sh -> publish_postgres.sh"

export DB_HOST="${SUPABASE_DB_HOST}"
export DB_NAME="${SUPABASE_DB_NAME}"
export DB_USER="${SUPABASE_DB_USER}"
export DB_PASSWORD="${SUPABASE_DB_PASSWORD}"
export DB_PORT="${SUPABASE_DB_PORT}"
export DB_SSLMODE="${SUPABASE_DB_SSLMODE:-require}"
export DB_SCHEMA="analytics"

exec "$(dirname "$0")/publish_postgres.sh"

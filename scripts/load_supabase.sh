#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Starting publish to Supabase..."

: "${PROJECT_ROOT:?PROJECT_ROOT not set}"
: "${SUPABASE_DB_HOST:?SUPABASE_DB_HOST not set}"
: "${SUPABASE_DB_NAME:?SUPABASE_DB_NAME not set}"
: "${SUPABASE_DB_USER:?SUPABASE_DB_USER not set}"
: "${SUPABASE_DB_PASSWORD:?SUPABASE_DB_PASSWORD not set}"
: "${SUPABASE_DB_PORT:?SUPABASE_DB_PORT not set}"

SSL_MODE="${SUPABASE_DB_SSLMODE:-require}"

EXPORT_DIR="${PROJECT_ROOT}/data/gold_exports"
KPI_DAILY="${EXPORT_DIR}/kpi_daily.csv"
KPI_MONTHLY="${EXPORT_DIR}/kpi_monthly.csv"
KPI_BY_STATE="${EXPORT_DIR}/kpi_by_state.csv"
PAYMENT_MIX="${EXPORT_DIR}/payment_mix.csv"
TOP_CATEGORIES="${EXPORT_DIR}/top_categories.csv"

for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES"; do
  if [[ ! -f "$f" ]]; then
    echo "[ERROR] Missing export file: $f"
    exit 1
  fi
done

export PGPASSWORD="${SUPABASE_DB_PASSWORD}"
export PGSSLMODE="${SSL_MODE}"

CONN="host=${SUPABASE_DB_HOST} port=${SUPABASE_DB_PORT} dbname=${SUPABASE_DB_NAME} user=${SUPABASE_DB_USER}"

echo "[INFO] Testing DNS resolve..."
getent hosts "${SUPABASE_DB_HOST}" >/dev/null || {
  echo "[ERROR] Cannot resolve host: ${SUPABASE_DB_HOST}"
  exit 2
}

echo "[INFO] Quick CSV sanity check (first 3 lines each)..."
for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES"; do
  echo "---- $(basename "$f")"
  head -n 3 "$f" || true
done

echo "[INFO] Truncating analytics tables..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "SET statement_timeout='5min';" >/dev/null

psql "${CONN}" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE
  analytics.kpi_daily,
  analytics.kpi_monthly,
  analytics.kpi_by_state,
  analytics.payment_mix,
  analytics.top_categories;
SQL

echo "[INFO] Loading KPI Daily..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_daily(date,revenue,orders,aov) FROM '${KPI_DAILY}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading KPI Monthly..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_monthly(month,revenue,orders,aov,mom_revenue_growth,mom_orders_growth) FROM '${KPI_MONTHLY}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading KPI by State..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_by_state(month,customer_state,revenue,orders) FROM '${KPI_BY_STATE}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading Payment Mix..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.payment_mix(month,payment_type,total_payment_value,share) FROM '${PAYMENT_MIX}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading Top Categories..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.top_categories(month,product_category_name,revenue,orders,avg_price,avg_freight,revenue_rank) FROM '${TOP_CATEGORIES}' WITH (FORMAT csv, HEADER true)"

echo "[OK] Publish to Supabase completed successfully."
#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Starting publish to Supabase..."

: "${PROJECT_ROOT:?PROJECT_ROOT not set}"
: "${SUPABASE_DB_HOST:?SUPABASE_DB_HOST not set}"
: "${SUPABASE_DB_NAME:?SUPABASE_DB_NAME not set}"
: "${SUPABASE_DB_USER:?SUPABASE_DB_USER not set}"
: "${SUPABASE_DB_PASSWORD:?SUPABASE_DB_PASSWORD not set}"
: "${SUPABASE_DB_PORT:?SUPABASE_DB_PORT not set}"

SSL_MODE="${SUPABASE_DB_SSLMODE:-require}"

EXPORT_DIR="${PROJECT_ROOT}/data/gold_exports"
KPI_DAILY="${EXPORT_DIR}/kpi_daily.csv"
KPI_MONTHLY="${EXPORT_DIR}/kpi_monthly.csv"
KPI_BY_STATE="${EXPORT_DIR}/kpi_by_state.csv"
PAYMENT_MIX="${EXPORT_DIR}/payment_mix.csv"
TOP_CATEGORIES="${EXPORT_DIR}/top_categories.csv"

for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES"; do
  if [[ ! -f "$f" ]]; then
    echo "[ERROR] Missing export file: $f"
    exit 1
  fi
done

export PGPASSWORD="${SUPABASE_DB_PASSWORD}"
export PGSSLMODE="${SSL_MODE}"

CONN="host=${SUPABASE_DB_HOST} port=${SUPABASE_DB_PORT} dbname=${SUPABASE_DB_NAME} user=${SUPABASE_DB_USER}"

echo "[INFO] Truncating analytics tables..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE
  analytics.kpi_daily,
  analytics.kpi_monthly,
  analytics.kpi_by_state,
  analytics.payment_mix,
  analytics.top_categories;
SQL

echo "[INFO] Loading KPI Daily..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_daily(date,revenue,orders,aov) FROM '${KPI_DAILY}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading KPI Monthly..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_monthly(month,revenue,orders,aov) FROM '${KPI_MONTHLY}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading KPI by State..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_by_state(month,customer_state,revenue,orders) FROM '${KPI_BY_STATE}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading Payment Mix..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.payment_mix(month,payment_type,total_payment_value,share) FROM '${PAYMENT_MIX}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading Top Categories..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "\copy analytics.top_categories(month,product_category_name,revenue,orders,avg_price,avg_freight,revenue_rank) FROM '${TOP_CATEGORIES}' WITH (FORMAT csv, HEADER true)"

echo "[OK] Publish to Supabase completed successfully."

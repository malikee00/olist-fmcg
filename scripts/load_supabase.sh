#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Starting publish to Supabase..."

# =========================
# Required env vars
# =========================
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

echo "[INFO] Checking CSV exports..."
for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES"; do
  if [[ ! -f "$f" ]]; then
    echo "[ERROR] Missing export file: $f"
    exit 1
  fi
done

export PGPASSWORD="${SUPABASE_DB_PASSWORD}"

PSQL="psql -h ${SUPABASE_DB_HOST} -p ${SUPABASE_DB_PORT} -U ${SUPABASE_DB_USER} -d ${SUPABASE_DB_NAME} sslmode=${SSL_MODE}"

echo "[INFO] Truncating analytics tables..."
$PSQL -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE TABLE
  analytics.kpi_daily,
  analytics.kpi_monthly,
  analytics.kpi_by_state,
  analytics.payment_mix,
  analytics.top_categories;
SQL

echo "[INFO] Loading KPI Daily..."
$PSQL -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_daily(date,revenue,orders,aov) FROM '${KPI_DAILY}' CSV HEADER"

echo "[INFO] Loading KPI Monthly..."
$PSQL -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_monthly(month,revenue,orders,aov,mom_revenue_growth,mom_orders_growth) FROM '${KPI_MONTHLY}' CSV HEADER"

echo "[INFO] Loading KPI by State..."
$PSQL -v ON_ERROR_STOP=1 -c "\copy analytics.kpi_by_state(month,customer_state,revenue,orders) FROM '${KPI_BY_STATE}' CSV HEADER"

echo "[INFO] Loading Payment Mix..."
$PSQL -v ON_ERROR_STOP=1 -c "\copy analytics.payment_mix(month,payment_type,total_payment_value,share) FROM '${PAYMENT_MIX}' CSV HEADER"

echo "[INFO] Loading Top Categories..."
$PSQL -v ON_ERROR_STOP=1 -c "\copy analytics.top_categories(month,product_category_name,revenue,orders,avg_price,avg_freight,revenue_rank) FROM '${TOP_CATEGORIES}' CSV HEADER"

echo "[OK] Publish to Supabase completed successfully."

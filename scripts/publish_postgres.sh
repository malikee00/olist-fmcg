#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Starting publish to PostgreSQL (Azure/Supabase/etc)..."

# ============================================================
# REQUIRED ENV
# ============================================================
: "${PROJECT_ROOT:?PROJECT_ROOT not set}"
: "${DB_HOST:?DB_HOST not set}"
: "${DB_NAME:?DB_NAME not set}"
: "${DB_USER:?DB_USER not set}"
: "${DB_PASSWORD:?DB_PASSWORD not set}"
: "${DB_PORT:?DB_PORT not set}"

# ============================================================
# OPTIONAL ENV
# ============================================================
DB_SCHEMA="${DB_SCHEMA:-analytics}"
SSL_MODE="${DB_SSLMODE:-require}"

EXPORT_DIR="${PROJECT_ROOT}/data/gold_exports"
KPI_DAILY="${EXPORT_DIR}/kpi_daily.csv"
KPI_MONTHLY="${EXPORT_DIR}/kpi_monthly.csv"
KPI_BY_STATE="${EXPORT_DIR}/kpi_by_state.csv"
PAYMENT_MIX="${EXPORT_DIR}/payment_mix.csv"
TOP_CATEGORIES="${EXPORT_DIR}/top_categories.csv"

# ============================================================
# VALIDATE EXPORT FILES
# ============================================================
for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES"; do
  if [[ ! -f "$f" ]]; then
    echo "[ERROR] Missing export file: $f"
    exit 1
  fi
done

# ============================================================
# PSQL CONNECTION
# ============================================================
export PGPASSWORD="${DB_PASSWORD}"
export PGSSLMODE="${SSL_MODE}"

CONN="host=${DB_HOST} port=${DB_PORT} dbname=${DB_NAME} user=${DB_USER}"

echo "[INFO] Connection target:"
echo "       host=${DB_HOST}"
echo "       port=${DB_PORT}"
echo "       dbname=${DB_NAME}"
echo "       user=${DB_USER}"
echo "       sslmode=${SSL_MODE}"
echo "       schema=${DB_SCHEMA}"

# DNS check
if command -v getent >/dev/null 2>&1; then
  echo "[INFO] Testing DNS resolve..."
  getent hosts "${DB_HOST}" >/dev/null || {
    echo "[ERROR] Cannot resolve host: ${DB_HOST}"
    exit 2
  }
else
  echo "[WARN] getent not found; skipping DNS resolve test."
fi

echo "[INFO] Testing DB connection..."
psql "${CONN}" -v ON_ERROR_STOP=1 -c "select 1 as ok;" >/dev/null

echo "[INFO] Quick CSV sanity check (first 3 lines each)..."
for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES"; do
  echo "---- $(basename "$f")"
  head -n 3 "$f" || true
done

echo "[INFO] Ensuring schema exists: ${DB_SCHEMA}"
psql "${CONN}" -v ON_ERROR_STOP=1 -c "create schema if not exists ${DB_SCHEMA};"

# ============================================================
# PREP STAGING TABLES (PERSISTENT)
# ============================================================
echo "[INFO] Preparing staging tables..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
CREATE TABLE IF NOT EXISTS ${DB_SCHEMA}._stg_kpi_monthly (
  month   text,
  revenue double precision,
  orders  bigint,
  aov     double precision
);

CREATE TABLE IF NOT EXISTS ${DB_SCHEMA}._stg_kpi_by_state (
  month          text,
  customer_state text,
  revenue        double precision,
  orders         bigint
);

CREATE TABLE IF NOT EXISTS ${DB_SCHEMA}._stg_payment_mix (
  month               text,
  payment_type        text,
  total_payment_value double precision,
  share               double precision
);

CREATE TABLE IF NOT EXISTS ${DB_SCHEMA}._stg_top_categories (
  month                 text,
  product_category_name  text,
  revenue                double precision,
  orders                 bigint,
  avg_price              double precision,
  avg_freight            double precision
);

TRUNCATE TABLE
  ${DB_SCHEMA}._stg_kpi_monthly,
  ${DB_SCHEMA}._stg_kpi_by_state,
  ${DB_SCHEMA}._stg_payment_mix,
  ${DB_SCHEMA}._stg_top_categories;
SQL

# ============================================================
# TRUNCATE TARGET TABLES (rerunnable)
# ============================================================
echo "[INFO] Truncating ${DB_SCHEMA} target tables..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
TRUNCATE TABLE
  ${DB_SCHEMA}.kpi_daily,
  ${DB_SCHEMA}.kpi_monthly,
  ${DB_SCHEMA}.kpi_by_state,
  ${DB_SCHEMA}.payment_mix,
  ${DB_SCHEMA}.top_categories;
SQL

# ============================================================
# LOAD KPI DAILY (direct)
# ============================================================
echo "[INFO] Loading KPI Daily..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}.kpi_daily(date,revenue,orders,aov) FROM '${KPI_DAILY}' WITH (FORMAT csv, HEADER true)"

# ============================================================
# LOAD KPI MONTHLY (CSV -> staging -> final, generate month_date)
# ============================================================
echo "[INFO] Loading KPI Monthly (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_kpi_monthly(month,revenue,orders,aov) FROM '${KPI_MONTHLY}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading KPI Monthly (staging -> final, generating month_date)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.kpi_monthly (month, month_date, revenue, orders, aov)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  revenue,
  orders,
  aov
FROM ${DB_SCHEMA}._stg_kpi_monthly;
SQL

# ============================================================
# LOAD KPI BY STATE (CSV -> staging -> final, generate month_date)
# ============================================================
echo "[INFO] Loading KPI by State (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_kpi_by_state(month,customer_state,revenue,orders) FROM '${KPI_BY_STATE}' WITH (FORMAT csv, HEADER true)"

echo "[INFO] Loading KPI by State (staging -> final, generating month_date)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.kpi_by_state (month, month_date, customer_state, revenue, orders)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  customer_state,
  revenue,
  orders
FROM ${DB_SCHEMA}._stg_kpi_by_state;
SQL

# ============================================================
# LOAD PAYMENT MIX (CSV -> staging -> final, generate month_date)
# ============================================================
echo "[INFO] Loading Payment Mix (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_payment_mix(month,payment_type,total_payment_value,share) FROM '${PAYMENT_MIX}' WITH (FORMAT csv, HEADER true, NULL '')"

echo "[INFO] Loading Payment Mix (staging -> final, generating month_date)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.payment_mix (month, month_date, payment_type, total_payment_value, share)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  payment_type,
  total_payment_value,
  share
FROM ${DB_SCHEMA}._stg_payment_mix;
SQL

# ============================================================
# LOAD TOP CATEGORIES (CSV -> staging -> final, generate month_date)
# ============================================================
echo "[INFO] Loading Top Categories (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_top_categories(month,product_category_name,revenue,orders,avg_price,avg_freight) FROM '${TOP_CATEGORIES}' WITH (FORMAT csv, HEADER true, NULL '')"

echo "[INFO] Loading Top Categories (staging -> final, generating month_date)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.top_categories (month, month_date, product_category_name, revenue, orders, avg_price, avg_freight)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  product_category_name,
  revenue,
  orders,
  avg_price,
  avg_freight
FROM ${DB_SCHEMA}._stg_top_categories;
SQL

echo "[OK] Publish to PostgreSQL completed successfully."

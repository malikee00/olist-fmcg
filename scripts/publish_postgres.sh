#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Starting publish to PostgreSQL (Azure/Supabase/etc)..."

# ============================================================
# REQUIRED ENV
# ============================================================
: "${PROJECT_ROOT:?PROJECT_ROOT not set}"

# ------------------------------------------------------------
# Compatibility layer:
# - If user uses AZURE_DB_* (as in your .env), but script expects DB_*,
#   we auto-map them when DB_* are missing.
# ------------------------------------------------------------
if [[ -z "${DB_HOST:-}" && -n "${AZURE_DB_HOST:-}" ]]; then
  export DB_HOST="${AZURE_DB_HOST}"
  export DB_PORT="${AZURE_DB_PORT:-5432}"
  export DB_NAME="${AZURE_DB_NAME}"
  export DB_USER="${AZURE_DB_USER}"
  export DB_PASSWORD="${AZURE_DB_PASSWORD}"
  export DB_SSLMODE="${AZURE_DB_SSLMODE:-require}"
  export DB_SCHEMA="${AZURE_DB_SCHEMA:-analytics}"
  echo "[INFO] Mapped AZURE_DB_* → DB_* for this run."
fi

: "${DB_HOST:?DB_HOST not set (or AZURE_DB_HOST not set)}"
: "${DB_NAME:?DB_NAME not set (or AZURE_DB_NAME not set)}"
: "${DB_USER:?DB_USER not set (or AZURE_DB_USER not set)}"
: "${DB_PASSWORD:?DB_PASSWORD not set (or AZURE_DB_PASSWORD not set)}"
: "${DB_PORT:?DB_PORT not set (or AZURE_DB_PORT not set)}"

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
PBI_FACT_DAILY="${EXPORT_DIR}/pbi_fact_daily.csv"

# ============================================================
# VALIDATE EXPORT FILES
# ============================================================
for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES" "$PBI_FACT_DAILY"; do
  if [[ ! -f "$f" ]]; then
    echo "[ERROR] Missing export file: $f"
    echo "[HINT] Run: bash scripts/export_gold_csv.sh"
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

# DNS check (optional)
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
for f in "$KPI_DAILY" "$KPI_MONTHLY" "$KPI_BY_STATE" "$PAYMENT_MIX" "$TOP_CATEGORIES" "$PBI_FACT_DAILY"; do
  echo "---- $(basename "$f")"
  head -n 3 "$f" || true
done

echo "[INFO] Ensuring schema exists: ${DB_SCHEMA}"
psql "${CONN}" -v ON_ERROR_STOP=1 -c "create schema if not exists ${DB_SCHEMA};"

# ============================================================
# GUARD: target tables must exist (created by sql/schema/00_init.sql)
# ============================================================
echo "[INFO] Checking target tables exist..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL >/dev/null
DO \$\$
DECLARE
  missing text[];
BEGIN
  missing := ARRAY[]::text[];

  IF to_regclass('${DB_SCHEMA}.kpi_daily') IS NULL THEN missing := array_append(missing, '${DB_SCHEMA}.kpi_daily'); END IF;
  IF to_regclass('${DB_SCHEMA}.kpi_monthly') IS NULL THEN missing := array_append(missing, '${DB_SCHEMA}.kpi_monthly'); END IF;
  IF to_regclass('${DB_SCHEMA}.kpi_by_state') IS NULL THEN missing := array_append(missing, '${DB_SCHEMA}.kpi_by_state'); END IF;
  IF to_regclass('${DB_SCHEMA}.payment_mix') IS NULL THEN missing := array_append(missing, '${DB_SCHEMA}.payment_mix'); END IF;
  IF to_regclass('${DB_SCHEMA}.top_categories') IS NULL THEN missing := array_append(missing, '${DB_SCHEMA}.top_categories'); END IF;
  IF to_regclass('${DB_SCHEMA}.pbi_fact_daily') IS NULL THEN missing := array_append(missing, '${DB_SCHEMA}.pbi_fact_daily'); END IF;

  IF array_length(missing, 1) IS NOT NULL THEN
    RAISE EXCEPTION 'Missing target tables: % . Run: psql ... -f sql/schema/00_init.sql', missing;
  END IF;
END
\$\$;
SQL

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
  month                  text,
  product_category_name  text,
  revenue                double precision,
  orders                 bigint,
  avg_price              double precision,
  avg_freight            double precision
);

CREATE TABLE IF NOT EXISTS ${DB_SCHEMA}._stg_pbi_fact_daily (
  fact_id               text,
  date                  text,
  month_date            text,
  customer_state        text,
  payment_type          text,
  product_category_name text,
  revenue               double precision,
  orders                bigint,
  total_payment_value   double precision,
  avg_price             double precision,
  avg_freight           double precision,
  batch_id              text,
  updated_at            text
);

TRUNCATE TABLE
  ${DB_SCHEMA}._stg_kpi_monthly,
  ${DB_SCHEMA}._stg_kpi_by_state,
  ${DB_SCHEMA}._stg_payment_mix,
  ${DB_SCHEMA}._stg_top_categories,
  ${DB_SCHEMA}._stg_pbi_fact_daily;
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
  ${DB_SCHEMA}.top_categories,
  ${DB_SCHEMA}.pbi_fact_daily;
SQL

# ============================================================
# LOAD KPI DAILY (direct)
# ============================================================
echo "[INFO] Loading KPI Daily..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}.kpi_daily(date,revenue,orders,aov) FROM '${KPI_DAILY}' WITH (FORMAT csv, HEADER true)"

# ============================================================
# LOAD KPI MONTHLY (CSV -> staging -> final)
# ============================================================
echo "[INFO] Loading KPI Monthly (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_kpi_monthly(month,revenue,orders,aov) FROM '${KPI_MONTHLY}' WITH (FORMAT csv, HEADER true, NULL '')"

echo "[INFO] Loading KPI Monthly (staging -> final)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.kpi_monthly (month, month_date, revenue, orders, aov)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(orders, 0) AS orders,
  COALESCE(aov, 0) AS aov
FROM ${DB_SCHEMA}._stg_kpi_monthly;
SQL

# ============================================================
# LOAD KPI BY STATE (CSV -> staging -> final)
# ============================================================
echo "[INFO] Loading KPI by State (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_kpi_by_state(month,customer_state,revenue,orders) FROM '${KPI_BY_STATE}' WITH (FORMAT csv, HEADER true, NULL '')"

echo "[INFO] Loading KPI by State (staging -> final)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.kpi_by_state (month, month_date, customer_state, revenue, orders)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  COALESCE(NULLIF(customer_state, ''), 'unknown') AS customer_state,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(orders, 0) AS orders
FROM ${DB_SCHEMA}._stg_kpi_by_state;
SQL

# ============================================================
# LOAD PAYMENT MIX (CSV -> staging -> final)
# ============================================================
echo "[INFO] Loading Payment Mix (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_payment_mix(month,payment_type,total_payment_value,share) FROM '${PAYMENT_MIX}' WITH (FORMAT csv, HEADER true, NULL '')"

echo "[INFO] Loading Payment Mix (staging -> final)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.payment_mix (month, month_date, payment_type, total_payment_value, share)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  COALESCE(NULLIF(payment_type, ''), 'unknown') AS payment_type,
  COALESCE(total_payment_value, 0) AS total_payment_value,
  COALESCE(share, 0) AS share
FROM ${DB_SCHEMA}._stg_payment_mix;
SQL

# ============================================================
# LOAD TOP CATEGORIES (CSV -> staging -> final)
# ============================================================
echo "[INFO] Loading Top Categories (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_top_categories(month,product_category_name,revenue,orders,avg_price,avg_freight) FROM '${TOP_CATEGORIES}' WITH (FORMAT csv, HEADER true, NULL '')"

echo "[INFO] Loading Top Categories (staging -> final)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.top_categories (month, month_date, product_category_name, revenue, orders, avg_price, avg_freight)
SELECT
  month,
  to_date(month || '-01', 'YYYY-MM-DD') AS month_date,
  COALESCE(NULLIF(product_category_name, ''), 'unknown') AS product_category_name,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(orders, 0) AS orders,
  avg_price,
  avg_freight
FROM ${DB_SCHEMA}._stg_top_categories;
SQL

# ============================================================
# LOAD PBI FACT DAILY (CSV -> staging -> final)
# ============================================================
echo "[INFO] Loading PBI Fact Daily (CSV -> staging)..."
psql "${CONN}" -v ON_ERROR_STOP=1 \
  -c "\copy ${DB_SCHEMA}._stg_pbi_fact_daily(fact_id,date,month_date,customer_state,payment_type,product_category_name,revenue,orders,total_payment_value,avg_price,avg_freight,batch_id,updated_at) FROM '${PBI_FACT_DAILY}' WITH (FORMAT csv, HEADER true, NULL '')"

echo "[INFO] Loading PBI Fact Daily (staging -> final)..."
psql "${CONN}" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO ${DB_SCHEMA}.pbi_fact_daily
(
  fact_id,
  date,
  month_date,
  customer_state,
  payment_type,
  product_category_name,
  revenue,
  orders,
  total_payment_value,
  avg_price,
  avg_freight,
  batch_id,
  updated_at
)
SELECT
  NULLIF(fact_id, '') AS fact_id,
  to_date(date, 'YYYY-MM-DD') AS date,
  to_date(month_date, 'YYYY-MM-DD') AS month_date,
  COALESCE(NULLIF(customer_state, ''), 'unknown') AS customer_state,
  COALESCE(NULLIF(payment_type, ''), 'unknown') AS payment_type,
  COALESCE(NULLIF(product_category_name, ''), 'unknown') AS product_category_name,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(orders, 0) AS orders,
  COALESCE(total_payment_value, 0) AS total_payment_value,
  avg_price,
  avg_freight,
  COALESCE(NULLIF(batch_id, ''), 'unknown') AS batch_id,
  COALESCE(NULLIF(updated_at, '')::timestamptz, now()) AS updated_at
FROM ${DB_SCHEMA}._stg_pbi_fact_daily;
SQL

echo "[OK] Publish to PostgreSQL completed successfully."
echo "[INFO] Row counts:"
psql "${CONN}" -v ON_ERROR_STOP=1 -c "select 'kpi_daily' as table, count(*) from ${DB_SCHEMA}.kpi_daily
union all select 'kpi_monthly', count(*) from ${DB_SCHEMA}.kpi_monthly
union all select 'kpi_by_state', count(*) from ${DB_SCHEMA}.kpi_by_state
union all select 'payment_mix', count(*) from ${DB_SCHEMA}.payment_mix
union all select 'top_categories', count(*) from ${DB_SCHEMA}.top_categories
union all select 'pbi_fact_daily', count(*) from ${DB_SCHEMA}.pbi_fact_daily;"

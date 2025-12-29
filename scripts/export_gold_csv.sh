#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Exporting Gold Parquet → CSV"

: "${PROJECT_ROOT:?PROJECT_ROOT not set}"

GOLD_DIR="${PROJECT_ROOT}/data/gold/olist_parquet"
EXPORT_DIR="${PROJECT_ROOT}/data/gold_exports"

mkdir -p "${EXPORT_DIR}"

SPARK_SUBMIT="spark-submit"

run_export () {
  local NAME="$1"
  local SRC="$2"
  local OUT="$3"

  if [[ ! -d "$SRC" ]]; then
    echo "[ERROR] Gold table path not found: $SRC"
    exit 1
  fi

  echo "[INFO] Exporting ${NAME} from ${SRC}"
  ${SPARK_SUBMIT} <<EOF
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("export_${NAME}_csv").getOrCreate()

df = spark.read.parquet("${SRC}")
(
  df
  .coalesce(1)
  .write
  .mode("overwrite")
  .option("header", "true")
  .csv("${OUT}")
)

spark.stop()
EOF
}

run_export "kpi_daily"        "${GOLD_DIR}/kpi_daily"        "${EXPORT_DIR}/kpi_daily_tmp"
run_export "kpi_monthly"     "${GOLD_DIR}/kpi_monthly"     "${EXPORT_DIR}/kpi_monthly_tmp"
run_export "kpi_by_state"    "${GOLD_DIR}/kpi_by_state"    "${EXPORT_DIR}/kpi_by_state_tmp"
run_export "payment_mix"     "${GOLD_DIR}/payment_mix"     "${EXPORT_DIR}/payment_mix_tmp"
run_export "top_categories"  "${GOLD_DIR}/top_products"    "${EXPORT_DIR}/top_categories_tmp"

echo "[INFO] Normalizing CSV filenames..."

for d in ${EXPORT_DIR}/*_tmp; do
  name=$(basename "$d" _tmp)
  csv=$(ls "$d"/*.csv | head -n 1)
  mv "$csv" "${EXPORT_DIR}/${name}.csv"
  rm -rf "$d"
done

echo "[OK] Gold export completed."
ls -lh "${EXPORT_DIR}"

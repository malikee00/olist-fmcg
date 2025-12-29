#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Phase 2 (Bronze) starting..."

cd /opt/project

SETTINGS="configs/settings.yaml"
BRONZE_TABLES="configs/bronze_tables.yaml"

if [ ! -f "$SETTINGS" ]; then
  echo "[ERROR] settings not found: $SETTINGS"
  exit 1
fi

if [ ! -f "$BRONZE_TABLES" ]; then
  echo "[ERROR] bronze_tables not found: $BRONZE_TABLES"
  exit 1
fi

echo "[INFO] Using settings      : $SETTINGS"
echo "[INFO] Using bronze tables : $BRONZE_TABLES"

# IMPORTANT: pass required args
python spark_jobs/bronze_ingest.py --settings "$SETTINGS" --bronze-tables "$BRONZE_TABLES"
python spark_jobs/bronze_validate.py --settings "$SETTINGS" --bronze-tables "$BRONZE_TABLES"

echo "[OK] Phase 2 done."

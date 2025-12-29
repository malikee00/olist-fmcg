#!/usr/bin/env bash
set -euo pipefail

BATCH_ID=$(python3 -c "import json; print(json.load(open('data/checkpoints/bronze_ingest_state.json'))['last_batch_id'])")

echo "[INFO] Phase 4 (Gold) starting for Batch: $BATCH_ID"
cd /opt/project

SETTINGS="configs/settings.yaml"
KPIS="configs/gold_kpis.yaml"

python spark_jobs/gold_aggregate.py --settings "$SETTINGS" --kpis "$KPIS" --batch_id "$BATCH_ID"
python spark_jobs/gold_validate.py --settings "$SETTINGS" --kpis "$KPIS" --batch_id "$BATCH_ID"

echo "[OK] Phase 4 done."
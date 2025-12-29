#!/usr/bin/env bash
set -euo pipefail

BATCH_ID=$(python3 -c "import json; print(json.load(open('data/checkpoints/bronze_ingest_state.json'))['last_batch_id'])")

echo "[INFO] Phase 3 (Silver) starting for Batch: $BATCH_ID"
cd /opt/project

CONFIG="configs/silver_models.yaml"

python spark_jobs/silver_transform.py --config "$CONFIG" --batch_id "$BATCH_ID"
python spark_jobs/silver_validate.py --config "$CONFIG" --batch_id "$BATCH_ID"

echo "[OK] Phase 3 done."
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from configs.loaders import load_bronze_tables, load_settings

from pyspark.sql import SparkSession, functions as F


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def read_checkpoint_last_batch_id(checkpoint_dir: Path) -> str:
    state_path = checkpoint_dir / "bronze_ingest_state.json"
    if not state_path.exists():
        raise RuntimeError(f"Checkpoint state not found: {state_path}")

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        raise RuntimeError(f"Checkpoint state is invalid/corrupted: {state_path}")

    batch_id = state.get("last_batch_id")
    if not batch_id:
        raise RuntimeError(f"Checkpoint missing last_batch_id: {state_path}")

    return str(batch_id)


def partition_path(table_dir: Path, batch_id: str) -> Path:
    return table_dir / f"batch_id={batch_id}"


def check_required_columns(table_name: str, columns: List[str]) -> List[str]:
    """
    Return list of missing required columns (snake_case expected).
    """
    required_map = {
        "olist_orders_dataset": ["order_id", "customer_id", "order_purchase_timestamp"],
        "olist_order_items_dataset": ["order_id", "product_id", "price"],
    }
    req = required_map.get(table_name)
    if not req:
        return []
    missing = [c for c in req if c not in columns]
    return missing


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--settings", required=True, help="Path to configs/settings.yaml")
    parser.add_argument("--bronze-tables", required=True, help="Path to configs/bronze_tables.yaml")
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional: validate specific batch_id. If omitted, uses checkpoint last_batch_id.",
    )
    parser.add_argument(
        "--skip-required-cols",
        action="store_true",
        help="Skip required column checks for orders/items.",
    )
    args = parser.parse_args()

    settings = load_settings(args.settings)
    bronze_cfg = load_bronze_tables(args.bronze_tables)

    bronze_root = (PROJECT_ROOT / settings.bronze_dir).resolve()
    checkpoint_root = (PROJECT_ROOT / settings.checkpoint_dir).resolve()

    batch_id = args.batch_id or read_checkpoint_last_batch_id(checkpoint_root)

    spark = SparkSession.builder.appName("olist-bronze-validate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"[INFO] Bronze root : {bronze_root}")
        print(f"[INFO] Batch ID    : {batch_id}")

        failures: List[str] = []
        results: List[Dict[str, Any]] = []

        for t in bronze_cfg.tables:
            table_name = t.name
            table_dir = bronze_root / table_name
            part_dir = partition_path(table_dir, batch_id)

            if not table_dir.exists():
                failures.append(f"{table_name}: table dir missing: {table_dir}")
                continue

            if not part_dir.exists():
                failures.append(f"{table_name}: partition missing for batch: {part_dir}")
                continue

            # Read only this batch partition
            df = spark.read.parquet(str(part_dir))

            col_count = len(df.columns)
            if col_count <= 0:
                failures.append(f"{table_name}: col_count=0 (invalid)")
                continue

            row_count = df.count()
            if row_count <= 0:
                failures.append(f"{table_name}: row_count=0 (invalid)")
                continue

            missing_required = []
            if not args.skip_required_cols:
                missing_required = check_required_columns(table_name, df.columns)
                if missing_required:
                    failures.append(
                        f"{table_name}: missing required columns: {', '.join(missing_required)}"
                    )

            results.append(
                {
                    "table": table_name,
                    "partition": str(part_dir),
                    "row_count": row_count,
                    "col_count": col_count,
                    "missing_required": missing_required,
                }
            )

            print(f"[OK] {table_name}: rows={row_count}, cols={col_count}")

        if failures:
            print("\n[FAIL] Bronze validation failed:")
            for f in failures:
                print(f" - {f}")
            return 1

        print("\n[PASS] Bronze validation passed for batch:", batch_id)
        return 0

    except Exception as e:
        print(f"[ERROR] Bronze validation crashed: {e}")
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

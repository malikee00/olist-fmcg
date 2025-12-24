from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from configs.loaders import load_bronze_tables, load_settings

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------
# Batch / Marker Helpers
# -----------------------------
BATCH_MARKER_RE = re.compile(r"^_BATCH_(batch_\d{8}_\d{4})\.ready$")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def list_marker_files(incoming_dir: Path) -> List[Path]:
    if not incoming_dir.exists():
        return []
    return sorted(
        [
            p
            for p in incoming_dir.iterdir()
            if p.is_file() and p.name.startswith("_BATCH_") and p.name.endswith(".ready")
        ]
    )


def parse_batch_id_from_marker(marker_path: Path) -> str:
    m = BATCH_MARKER_RE.match(marker_path.name)
    if not m:
        raise RuntimeError(f"Invalid marker filename: {marker_path.name}")
    return m.group(1)


def select_latest_batch_marker(incoming_dir: Path) -> Optional[Path]:
    markers = list_marker_files(incoming_dir)
    if not markers:
        return None
    return sorted(markers, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def ensure_required_files_present(incoming_dir: Path, required_files: List[str]) -> None:
    missing = [f for f in required_files if not (incoming_dir / f).exists()]
    if missing:
        raise RuntimeError(
            f"Invalid batch: missing required CSV files in incoming/: {', '.join(missing)}"
        )


# -----------------------------
# Bronze Transform Helpers
# -----------------------------
def snake_case(name: str) -> str:
    s = name.strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"__+", "_", s)
    return s.lower().strip("_")


def normalize_columns(df: DataFrame) -> DataFrame:
    for c in df.columns:
        df = df.withColumnRenamed(c, snake_case(c))
    return df


def cast_columns(df: DataFrame, ts_cols: List[str], num_cols: List[str]) -> DataFrame:
    for c in ts_cols:
        c2 = snake_case(c)
        if c2 in df.columns:
            df = df.withColumn(c2, F.to_timestamp(F.col(c2), "yyyy-MM-dd HH:mm:ss"))

    for c in num_cols:
        c2 = snake_case(c)
        if c2 in df.columns:
            df = df.withColumn(c2, F.col(c2).cast("double"))

    return df


def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )


def write_bronze_table(
    df: DataFrame,
    out_root: Path,
    table_name: str,
    batch_id: str,
    partition_by: str,
    write_format: str,
    write_mode: str,
) -> Tuple[str, int]:
    """
    Returns (output_dir, row_count)
    """
    table_dir = out_root / table_name
    df_out = df.withColumn("batch_id", F.lit(batch_id))

    row_count = df_out.count()
    if row_count <= 0:
        raise RuntimeError(
            f"Invalid batch: table {table_name} produced 0 rows (not allowed)."
        )

    writer = df_out.write.mode(write_mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)

    if write_format == "parquet":
        writer.parquet(str(table_dir))
    else:
        raise RuntimeError(f"Unsupported write_format: {write_format}")

    return (str(table_dir), row_count)


def write_metadata(meta_dir: Path, batch_id: str, payload: Dict[str, Any]) -> Path:
    meta_dir.mkdir(parents=True, exist_ok=True)
    out_path = meta_dir / f"batch_{batch_id}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path

# -----------------------------
def default_state() -> Dict[str, Any]:
    return {
        "last_batch_id": None,
        "processed_markers": [],   
        "processed_batches": [],   
        "updated_at": None,
    }


def load_state(state_path: Path) -> Dict[str, Any]:
    if not state_path.exists():
        state_path.parent.mkdir(parents=True, exist_ok=True)
        s = default_state()
        save_state_atomic(state_path, s)
        return s

    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("state root is not a dict")
    except Exception:
        raise RuntimeError(f"Checkpoint state is invalid/corrupted: {state_path}")

    base = default_state()
    base.update(raw)
    if not isinstance(base.get("processed_markers"), list):
        base["processed_markers"] = []
    if not isinstance(base.get("processed_batches"), list):
        base["processed_batches"] = []
    return base


def save_state_atomic(state_path: Path, state: Dict[str, Any]) -> None:
    """
    Atomic write:
      write to tmp -> replace
    """
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(".tmp")

    tmp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(str(tmp_path), str(state_path))


def is_processed(state: Dict[str, Any], batch_id: str, marker_name: str) -> bool:
    if marker_name in state.get("processed_markers", []):
        return True
    if batch_id in state.get("processed_batches", []):
        return True
    return False


def mark_processed(
    state: Dict[str, Any],
    batch_id: str,
    marker_name: str,
    keep_last: int = 200,
) -> Dict[str, Any]:
    pm = state.get("processed_markers", [])
    pb = state.get("processed_batches", [])

    pm.append(marker_name)
    pb.append(batch_id)

    def uniq_keep_last(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        # keep order, then trim later
        for x in items:
            if x not in seen:
                seen.add(x)
                out.append(x)
        # keep last N
        if len(out) > keep_last:
            out = out[-keep_last:]
        return out

    state["processed_markers"] = uniq_keep_last(pm)
    state["processed_batches"] = uniq_keep_last(pb)
    state["last_batch_id"] = batch_id
    state["updated_at"] = now_iso()
    return state


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--settings", required=True, help="Path to configs/settings.yaml")
    parser.add_argument(
        "--bronze-tables", required=True, help="Path to configs/bronze_tables.yaml"
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional: force a specific batch_id (must match marker naming).",
    )
    parser.add_argument("--no-meta", action="store_true", help="Disable writing _meta JSON")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force ingest even if checkpoint says processed (use carefully).",
    )
    args = parser.parse_args()

    settings = load_settings(args.settings)
    bronze_cfg = load_bronze_tables(args.bronze_tables)

    incoming_dir = (PROJECT_ROOT / settings.raw_incoming_dir).resolve()
    bronze_root = (PROJECT_ROOT / settings.bronze_dir).resolve()
    meta_dir = bronze_root / "_meta"

    checkpoint_root = (PROJECT_ROOT / settings.checkpoint_dir).resolve()
    state_path = checkpoint_root / "bronze_ingest_state.json"

    spark = SparkSession.builder.appName("olist-bronze-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1) Detect batch
        marker = select_latest_batch_marker(incoming_dir)
        if marker is None:
            print("[NO-OP] No batch marker found in incoming/. Nothing to ingest.")
            return 0

        detected_batch_id = parse_batch_id_from_marker(marker)
        batch_id = args.batch_id or detected_batch_id
        if batch_id != detected_batch_id:
            raise RuntimeError(
                f"batch_id mismatch: provided={batch_id} but marker indicates={detected_batch_id}."
            )

        print(f"[INFO] Detected marker : {marker.name}")
        print(f"[INFO] Batch ID       : {batch_id}")
        print(f"[INFO] State path     : {state_path}")

        # 2.7) Checkpoint skip logic
        state = load_state(state_path)
        if (not args.force) and is_processed(state, batch_id=batch_id, marker_name=marker.name):
            print("[SKIP] Batch already processed according to checkpoint state.")
            print("       Use --force to re-run intentionally.")
            return 0

        # 2) Validate required files for atomic batch
        required_files = [t.file for t in bronze_cfg.tables]
        ensure_required_files_present(incoming_dir, required_files)

        # 3-5) Read, normalize, cast, write
        table_results: List[Dict[str, Any]] = []
        for t in bronze_cfg.tables:
            src_path = incoming_dir / t.file
            print(f"[INFO] Reading: {src_path.name}")

            df = read_csv(spark, src_path)
            df = normalize_columns(df)
            df = cast_columns(df, t.timestamp_cols, t.numeric_cols)

            out_dir, row_count = write_bronze_table(
                df=df,
                out_root=bronze_root,
                table_name=t.name,
                batch_id=batch_id,
                partition_by=bronze_cfg.partition_by,
                write_format=bronze_cfg.write_format,
                write_mode=bronze_cfg.write_mode,
            )

            print(f"[OK] Wrote {t.name} rows={row_count} -> {out_dir}")
            table_results.append(
                {
                    "table": t.name,
                    "source_file": t.file,
                    "rows": row_count,
                    "output_dir": out_dir,
                }
            )

        # 6) Metadata 
        if not args.no_meta:
            payload = {
                "batch_id": batch_id,
                "ingested_at": now_iso(),
                "incoming_dir": str(incoming_dir),
                "bronze_root": str(bronze_root),
                "marker_file": marker.name,
                "tables": table_results,
                "status": "success",
            }
            meta_path = write_metadata(meta_dir, batch_id, payload)
            print(f"[OK] Wrote metadata: {meta_path}")

        state = mark_processed(state, batch_id=batch_id, marker_name=marker.name)
        save_state_atomic(state_path, state)
        print(f"[OK] Updated checkpoint: {state_path}")

        print("[DONE] Bronze ingest complete.")
        return 0

    except Exception as e:
        print(f"[ERROR] Bronze ingest failed: {e}")
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

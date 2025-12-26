from __future__ import annotations

import argparse
import os
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =====================================================
# Helpers
# =====================================================
def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def safe_count(df) -> int:
    return int(df.count())


def coverage_ratio(df, col_name: str) -> float:
    """
    % rows where col is non-null and non-empty string (if string).
    Works for both string and non-string cols.
    """
    c = F.col(col_name)
    ok = F.when(c.isNotNull() & (F.trim(c.cast("string")) != ""), F.lit(1)).otherwise(F.lit(0))
    return float(df.select(ok.alias("ok")).agg(F.avg("ok").alias("cov")).collect()[0]["cov"])


def warn(msg: str) -> None:
    print(f"[WARN] {msg}")


# =====================================================
# Main validation
# =====================================================
def run(config_path: str, batch_id: str) -> None:
    cfg = read_yaml(config_path)

    output_dir = cfg["global"]["output_dir"]
    batch_key = cfg["global"].get("batch_key", "batch_id")

    quality = cfg.get("quality", {})
    min_category_cov = float(quality.get("min_category_coverage", 0.70))
    min_state_cov = float(quality.get("min_state_coverage", 0.80))

    global_drop_fail_ratio = quality.get("global_drop_fail_ratio")  
    prev_total_path = os.path.join(output_dir, "_meta", "global_counts.json")

    out = cfg["outputs"]
    fo_path = os.path.join(output_dir, out["fact_orders"]["path"])
    fi_path = os.path.join(output_dir, out["fact_order_items"]["path"])
    dc_path = os.path.join(output_dir, out["dim_customers"]["path"])
    dp_path = os.path.join(output_dir, out["dim_products"]["path"])

    spark = SparkSession.builder.appName("uplift_phase3_silver_validate").getOrCreate()

    # -------------------------
    # Batch-level checks (STRICT)
    # -------------------------
    if not os.path.exists(fo_path):
        fail(f"[SILVER] fact_orders path missing: {fo_path}")
    if not os.path.exists(fi_path):
        fail(f"[SILVER] fact_order_items path missing: {fi_path}")

    fo_all = spark.read.parquet(fo_path)
    fi_all = spark.read.parquet(fi_path)

    fo = fo_all.filter(F.col(batch_key) == F.lit(batch_id))
    fi = fi_all.filter(F.col(batch_key) == F.lit(batch_id))

    fo_cnt = safe_count(fo)
    fi_cnt = safe_count(fi)

    if fo_cnt <= 0:
        fail(f"[SILVER][BATCH] fact_orders empty for batch_id={batch_id}")
    if fi_cnt <= 0:
        fail(f"[SILVER][BATCH] fact_order_items empty for batch_id={batch_id}")

    # Key integrity
    if safe_count(fo.filter(F.col("order_id").isNull())) > 0:
        fail("[SILVER][BATCH] Null order_id in fact_orders")

    if safe_count(fi.filter(F.col("order_id").isNull() | F.col("order_item_id").isNull())) > 0:
        fail("[SILVER][BATCH] Null (order_id, order_item_id) in fact_order_items")

    # Uniqueness within batch
    if safe_count(fo.groupBy("order_id").count().filter(F.col("count") > 1)) > 0:
        fail("[SILVER][BATCH] Duplicate order_id in fact_orders within batch")

    if safe_count(fi.groupBy("order_id", "order_item_id").count().filter(F.col("count") > 1)) > 0:
        fail("[SILVER][BATCH] Duplicate (order_id, order_item_id) in fact_order_items within batch")

    # Join coverage (batch)
    if "product_category_name" in fi.columns:
        cat_cov = coverage_ratio(fi, "product_category_name")
        if cat_cov < min_category_cov:
            fail(f"[SILVER][BATCH] Category coverage too low: {cat_cov:.2%} (< {min_category_cov:.0%})")
    else:
        fail("[SILVER][BATCH] Missing product_category_name in fact_order_items (join likely failed)")

    if "customer_state" in fo.columns:
        state_cov = coverage_ratio(fo, "customer_state")
        if state_cov < min_state_cov:
            fail(f"[SILVER][BATCH] State coverage too low: {state_cov:.2%} (< {min_state_cov:.0%})")
    else:
        fail("[SILVER][BATCH] Missing customer_state in fact_orders (join likely failed)")

    # -------------------------
    # Global sanity (LIGHT)
    # -------------------------
    if not os.path.exists(dc_path):
        fail(f"[SILVER] dim_customers path missing: {dc_path}")
    if not os.path.exists(dp_path):
        fail(f"[SILVER] dim_products path missing: {dp_path}")

    dc = spark.read.parquet(dc_path)
    dp = spark.read.parquet(dp_path)

    dc_cnt = safe_count(dc)
    dp_cnt = safe_count(dp)

    if dc_cnt <= 0:
        fail("[SILVER][GLOBAL] dim_customers empty")
    if dp_cnt <= 0:
        fail("[SILVER][GLOBAL] dim_products empty")

    total_fo = safe_count(fo_all)
    total_fi = safe_count(fi_all)

    if os.path.exists(prev_total_path):
        try:
            import json

            with open(prev_total_path, "r", encoding="utf-8") as f:
                prev = json.load(f)

            prev_fo = int(prev.get("fact_orders_total", total_fo))
            prev_fi = int(prev.get("fact_order_items_total", total_fi))

            if global_drop_fail_ratio is not None:
                ratio = float(global_drop_fail_ratio)
                if prev_fo > 0 and total_fo < ratio * prev_fo:
                    fail(f"[SILVER][GLOBAL] fact_orders total dropped too much: {total_fo} < {ratio:.2f} * {prev_fo}")
                if prev_fi > 0 and total_fi < ratio * prev_fi:
                    fail(f"[SILVER][GLOBAL] fact_order_items total dropped too much: {total_fi} < {ratio:.2f} * {prev_fi}")
            else:
                # only warn
                if prev_fo > 0 and total_fo < 0.95 * prev_fo:
                    warn(f"fact_orders total decreased noticeably: {total_fo} vs prev {prev_fo}")
                if prev_fi > 0 and total_fi < 0.95 * prev_fi:
                    warn(f"fact_order_items total decreased noticeably: {total_fi} vs prev {prev_fi}")

        except Exception as e:
            warn(f"Failed reading global_counts.json for sanity check: {e}")

    try:
        import json

        meta_dir = os.path.join(output_dir, "_meta")
        os.makedirs(meta_dir, exist_ok=True)
        with open(prev_total_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "updated_at_utc": datetime.utcnow().isoformat(),
                    "fact_orders_total": total_fo,
                    "fact_order_items_total": total_fi,
                    "dim_customers_total": dc_cnt,
                    "dim_products_total": dp_cnt,
                },
                f,
                indent=2,
            )
    except Exception as e:
        warn(f"Failed writing global_counts.json: {e}")

    print(
        "[PASS] Silver validation OK for batch="
        f"{batch_id}. rows: fact_orders={fo_cnt}, fact_items={fi_cnt}. "
        f"coverage: category={cat_cov:.2%}, state={state_cov:.2%}."
    )

    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--batch_id", required=True)
    args = p.parse_args()
    run(args.config, args.batch_id)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import os
from typing import Optional

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def safe_count(df) -> int:
    return int(df.count())


def run(config_path: str, batch_id: str) -> None:
    cfg = read_yaml(config_path)

    output_dir = cfg["global"]["output_dir"]
    batch_key = cfg["global"].get("batch_key", "batch_id")
    min_cov = float(cfg.get("quality", {}).get("min_category_coverage", 0.70))

    # Output paths from config
    out = cfg["outputs"]
    fo_path = os.path.join(output_dir, out["fact_orders"]["path"])
    fi_path = os.path.join(output_dir, out["fact_order_items"]["path"])
    dc_path = os.path.join(output_dir, out["dim_customers"]["path"])
    dp_path = os.path.join(output_dir, out["dim_products"]["path"])

    spark = SparkSession.builder.appName("uplift_phase3_silver_validate").getOrCreate()

    # --- read batch facts
    if not os.path.exists(fo_path):
        fail(f"[SILVER] fact_orders path missing: {fo_path}")
    if not os.path.exists(fi_path):
        fail(f"[SILVER] fact_order_items path missing: {fi_path}")

    fo = spark.read.parquet(fo_path).filter(F.col(batch_key) == F.lit(batch_id))
    fi = spark.read.parquet(fi_path).filter(F.col(batch_key) == F.lit(batch_id))

    if safe_count(fo) <= 0:
        fail(f"[SILVER][BATCH] fact_orders empty for batch_id={batch_id}")
    if safe_count(fi) <= 0:
        fail(f"[SILVER][BATCH] fact_order_items empty for batch_id={batch_id}")

    # --- null checks (keys)
    if safe_count(fo.filter(F.col("order_id").isNull())) > 0:
        fail("[SILVER][BATCH] Null order_id in fact_orders")

    if safe_count(fi.filter(F.col("order_id").isNull() | F.col("order_item_id").isNull())) > 0:
        fail("[SILVER][BATCH] Null (order_id, order_item_id) in fact_order_items")

    # --- uniqueness checks within batch
    if safe_count(fo.groupBy("order_id").count().filter(F.col("count") > 1)) > 0:
        fail("[SILVER][BATCH] Duplicate order_id in fact_orders within batch")

    if safe_count(fi.groupBy("order_id", "order_item_id").count().filter(F.col("count") > 1)) > 0:
        fail("[SILVER][BATCH] Duplicate (order_id, order_item_id) in fact_order_items within batch")

    # --- category coverage check (fact_order_items)
    cov_row = (
        fi.select(
            F.when(
                F.col("product_category_name").isNotNull() & (F.col("product_category_name") != ""),
                F.lit(1)
            ).otherwise(F.lit(0)).alias("has_cat")
        )
        .agg(F.avg("has_cat").alias("coverage"))
        .collect()
    )
    cov = cov_row[0]["coverage"] if cov_row else None
    if cov is None:
        fail("[SILVER][BATCH] Failed computing category coverage")
    if float(cov) < min_cov:
        fail(f"[SILVER][BATCH] Category coverage too low: {float(cov):.2%} (< {min_cov:.0%})")

    # --- dims sanity 
    if not os.path.exists(dc_path):
        fail(f"[SILVER] dim_customers path missing: {dc_path}")
    if not os.path.exists(dp_path):
        fail(f"[SILVER] dim_products path missing: {dp_path}")

    dc = spark.read.parquet(dc_path)
    dp = spark.read.parquet(dp_path)

    if safe_count(dc) <= 0:
        fail("[SILVER][GLOBAL] dim_customers empty")
    if safe_count(dp) <= 0:
        fail("[SILVER][GLOBAL] dim_products empty")

    print(f"[PASS] Silver validation OK for batch={batch_id}. category_coverage={float(cov):.2%}")
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--batch_id", required=True)
    args = p.parse_args()
    run(args.config, args.batch_id)


if __name__ == "__main__":
    main()

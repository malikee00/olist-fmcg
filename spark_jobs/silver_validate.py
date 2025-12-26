from __future__ import annotations

import argparse
import os
import yaml

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def run(config_path: str, batch_id: str) -> None:
    cfg = read_yaml(config_path)
    silver_root = cfg["global"]["silver_root"]
    batch_col = cfg["global"].get("batch_col", "batch_id")
    min_cov = cfg.get("quality", {}).get("min_category_coverage", 0.70)

    spark = SparkSession.builder.appName("uplift_phase3_silver_validate").getOrCreate()

    fo_path = os.path.join(silver_root, cfg["silver"]["facts"]["fact_orders"]["path"])
    fi_path = os.path.join(silver_root, cfg["silver"]["facts"]["fact_order_items"]["path"])
    dc_path = os.path.join(silver_root, cfg["silver"]["dims"]["dim_customers"]["path"])
    dp_path = os.path.join(silver_root, cfg["silver"]["dims"]["dim_products"]["path"])

    # batch-level facts
    fo = spark.read.parquet(fo_path).filter(F.col(batch_col) == F.lit(batch_id))
    fi = spark.read.parquet(fi_path).filter(F.col(batch_col) == F.lit(batch_id))

    if fo.count() <= 0:
        fail(f"[SILVER][BATCH] fact_orders empty for batch_id={batch_id}")
    if fi.count() <= 0:
        fail(f"[SILVER][BATCH] fact_order_items empty for batch_id={batch_id}")

    # null checks
    if fo.filter(F.col("order_id").isNull()).count() > 0:
        fail("[SILVER][BATCH] Null order_id in fact_orders")

    if fi.filter(F.col("order_id").isNull() | F.col("order_item_id").isNull()).count() > 0:
        fail("[SILVER][BATCH] Null (order_id, order_item_id) in fact_order_items")

    # uniqueness checks within batch
    if fo.groupBy("order_id").count().filter(F.col("count") > 1).count() > 0:
        fail("[SILVER][BATCH] Duplicate order_id in fact_orders within batch")

    if fi.groupBy("order_id", "order_item_id").count().filter(F.col("count") > 1).count() > 0:
        fail("[SILVER][BATCH] Duplicate (order_id, order_item_id) in fact_order_items within batch")

    # coverage checks
    cov = (
        fi.select(
            F.when(
                F.col("product_category_name").isNotNull() & (F.col("product_category_name") != ""),
                F.lit(1)
            ).otherwise(F.lit(0)).alias("has_cat")
        )
        .agg(F.avg("has_cat").alias("coverage"))
        .collect()[0]["coverage"]
    )
    if cov is None:
        fail("[SILVER][BATCH] Failed computing category coverage")
    if float(cov) < float(min_cov):
        fail(f"[SILVER][BATCH] Category coverage too low: {float(cov):.2%} (< {float(min_cov):.0%})")

    # global sanity dims non-empty
    dc = spark.read.parquet(dc_path)
    dp = spark.read.parquet(dp_path)
    if dc.count() <= 0:
        fail("[SILVER][GLOBAL] dim_customers empty")
    if dp.count() <= 0:
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

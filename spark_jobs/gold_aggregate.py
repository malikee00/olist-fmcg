from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Dict, Any, List

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =====================================================
# Helpers
# =====================================================
def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def safe_cols(df: DataFrame, cols: List[str]) -> DataFrame:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing cols: {missing}. Found={df.columns}")
    return df.select(*cols)


def agg_from_config(df: DataFrame, group_by: List[str], metrics: List[Dict[str, Any]]) -> DataFrame:
    aggs = []
    for m in metrics:
        name = m["name"]
        op = m["op"]
        col = m.get("col")

        if op == "sum":
            aggs.append(F.sum(F.col(col)).alias(name))
        elif op == "avg":
            aggs.append(F.avg(F.col(col)).alias(name))
        elif op == "count":
            aggs.append(F.count(F.lit(1)).alias(name))
        elif op == "count_distinct":
            aggs.append(F.countDistinct(F.col(col)).alias(name))
        else:
            raise ValueError(f"Unsupported metric op: {op}")

    return df.groupBy(*group_by).agg(*aggs)


def apply_derived_metrics(df: DataFrame, derived: List[Dict[str, Any]]) -> DataFrame:
    out = df
    for d in derived:
        out = out.withColumn(d["name"], F.expr(d["expr"]))
    return out


def write_partition_overwrite(df: DataFrame, out_path: str, partition_by: List[str]) -> None:
    spark = df.sparkSession
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        df.write
        .mode("overwrite")
        .partitionBy(*partition_by)
        .parquet(out_path)
    )


# =====================================================
# Main
# =====================================================
def run(settings_path: str, kpis_path: str, batch_id: str) -> None:
    settings = read_yaml(settings_path)
    kpis = read_yaml(kpis_path)

    checkpoint_dir = settings["paths"]["checkpoint_dir"]
    silver_dir = settings["paths"]["silver_dir"]
    gold_dir = settings["paths"]["gold_dir"]

    # NOTE: filenames are fixed to your project convention (no settings["checkpoints"])
    gold_state_path = os.path.join(checkpoint_dir, "gold_aggregate_state.json")

    meta_dir = os.path.join(gold_dir, "_meta")
    ensure_dir(meta_dir)

    batch_key = kpis["global"].get("batch_key", "batch_id")
    delivered_only = bool(kpis.get("business_rules", {}).get("delivered_only", True))

    spark = SparkSession.builder.appName("uplift_phase4_gold_aggregate").getOrCreate()

    # ---- read Silver
    fact_orders_path = os.path.join(silver_dir, "fact_orders")
    fact_items_path = os.path.join(silver_dir, "fact_order_items")
    dim_products_path = os.path.join(silver_dir, "dim_products")
    dim_customers_path = os.path.join(silver_dir, "dim_customers")

    orders = spark.read.parquet(fact_orders_path)
    items = spark.read.parquet(fact_items_path)
    products = spark.read.parquet(dim_products_path)
    customers = spark.read.parquet(dim_customers_path)  

    # ---- affected window (from batch)
    orders_batch = orders.filter(F.col(batch_key) == F.lit(batch_id))

    if delivered_only and "order_status" in orders_batch.columns:
        orders_batch = orders_batch.filter(F.col("order_status") == F.lit("delivered"))

    if "order_purchase_timestamp" not in orders_batch.columns:
        raise ValueError("fact_orders must contain order_purchase_timestamp")

    orders_batch = (
        orders_batch
        .withColumn("date", F.to_date(F.col("order_purchase_timestamp")))
        .withColumn("month", F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"))
    )

    affected_dates = [r["date"].isoformat() for r in orders_batch.select("date").distinct().collect()]
    affected_months = [r["month"] for r in orders_batch.select("month").distinct().collect()]

    if not affected_dates:
        print(f"[NO-OP] No affected dates for batch_id={batch_id}")
        spark.stop()
        return

    # ---- build base filtered views
    orders_all = orders
    if delivered_only and "order_status" in orders_all.columns:
        orders_all = orders_all.filter(F.col("order_status") == F.lit("delivered"))

    orders_all = (
        orders_all
        .withColumn("date", F.to_date(F.col("order_purchase_timestamp")))
        .withColumn("month", F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"))
        .withColumn("payment_total_value", F.col("payment_total_value").cast("double"))
        .withColumn(
            "payment_type",
            F.when(F.col("payment_type_main").isNull() | (F.col("payment_type_main") == ""), F.lit("unknown"))
             .otherwise(F.col("payment_type_main"))
        )
    )

    orders_daily = orders_all.filter(F.col("date").isin(affected_dates))
    orders_monthly = orders_all.filter(F.col("month").isin(affected_months))

    wanted_items = [c for c in ["order_id", "order_item_id", "product_id", "price", "freight_value", batch_key] if c in items.columns]
    items_use = safe_cols(items, wanted_items).withColumn("price", F.col("price").cast("double")).withColumn("freight_value", F.col("freight_value").cast("double"))

    prod_use = products.select("product_id", "product_category_name")

    # base for daily (item-level)
    base_daily = (
        items_use
        .join(orders_daily.select("order_id", "date", "month", "customer_state", "customer_city"), on="order_id", how="inner")
        .join(prod_use, on="product_id", how="left")
        .withColumn(
            "product_category_name",
            F.when(F.col("product_category_name").isNull() | (F.col("product_category_name") == ""), F.lit("unknown"))
             .otherwise(F.col("product_category_name"))
        )
    )

    # base for monthly (item-level)
    base_monthly = (
        items_use
        .join(orders_monthly.select("order_id", "date", "month", "customer_state", "customer_city"), on="order_id", how="inner")
        .join(prod_use, on="product_id", how="left")
        .withColumn(
            "product_category_name",
            F.when(F.col("product_category_name").isNull() | (F.col("product_category_name") == ""), F.lit("unknown"))
             .otherwise(F.col("product_category_name"))
        )
    )

    # orders monthly view (order-level) for payment_mix
    orders_monthly_view = orders_monthly.select("month", "payment_type", "payment_total_value", "order_id")

    tables_cfg = kpis["tables"]
    rowcounts: Dict[str, int] = {}

    for key, tcfg in tables_cfg.items():
        if isinstance(tcfg, dict) and tcfg.get("enabled") is False:
            continue

        out_path = os.path.join(gold_dir, tcfg["path"])
        partition_by = tcfg["partition_by"]
        group_by = tcfg["group_by"]
        metrics = tcfg["metrics"]
        derived = tcfg.get("derived_metrics", [])

        src = tcfg["source"]
        if src == "base_items_daily":
            src_df = base_daily
        elif src == "base_items_monthly":
            src_df = base_monthly
        elif src == "orders_monthly":
            src_df = orders_monthly_view
        else:
            raise ValueError(f"Unknown source: {src}")

        agg_df = agg_from_config(src_df, group_by=group_by, metrics=metrics)
        agg_df = apply_derived_metrics(agg_df, derived)

        # payment_mix special: share
        if key == "payment_mix":
            w = Window.partitionBy("month")
            agg_df = agg_df.withColumn("month_total", F.sum("total_payment_value").over(w))
            agg_df = agg_df.withColumn(
                "share",
                F.expr("CASE WHEN month_total = 0 THEN 0 ELSE total_payment_value / month_total END")
            ).drop("month_total")

        write_partition_overwrite(agg_df, out_path, partition_by)
        rowcounts[key] = int(agg_df.count())

    # ---- meta
    meta_payload = {
        "batch_id": batch_id,
        "run_at_utc": datetime.utcnow().isoformat(),
        "window_strategy": kpis["incremental"]["window_strategy"],
        "affected_dates": affected_dates,
        "affected_months": affected_months,
        "rowcounts": rowcounts,
    }
    write_json(os.path.join(meta_dir, f"batch_{batch_id}.json"), meta_payload)

    # ---- checkpoint
    write_json(gold_state_path, {
        "last_processed_batch_id": batch_id,
        "updated_at": datetime.utcnow().isoformat(),
    })

    print(f"[OK] Gold aggregate completed for batch_id={batch_id}. affected_dates={len(affected_dates)}, affected_months={len(affected_months)}")
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--settings", required=True)
    p.add_argument("--kpis", required=True)
    p.add_argument("--batch_id", required=True)
    args = p.parse_args()
    run(args.settings, args.kpis, args.batch_id)


if __name__ == "__main__":
    main()

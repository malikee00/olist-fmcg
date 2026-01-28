# spark_jobs/gold_aggregate.py
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Dict, Any, List

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel


# ----------------------------
# Helpers
# ----------------------------
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


def apply_windows_bind_mount_safe_conf(spark: SparkSession) -> None:
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    hconf.set("parquet.enable.summary-metadata", "false")
    hconf.set("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")


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


def df_is_empty(df: DataFrame) -> bool:
    return len(df.take(1)) == 0


# ----------------------------
# Core
# ----------------------------
def run(settings_path: str, kpis_path: str, batch_id: str) -> None:
    settings = read_yaml(settings_path)
    kpis = read_yaml(kpis_path)

    checkpoint_dir = settings["paths"]["checkpoint_dir"]
    silver_dir = settings["paths"]["silver_dir"]
    gold_dir = settings["paths"]["gold_dir"]

    gold_state_path = os.path.join(checkpoint_dir, "gold_aggregate_state.json")
    meta_dir = os.path.join(gold_dir, "_meta")
    ensure_dir(meta_dir)

    batch_key = kpis.get("global", {}).get("batch_key", "batch_id")
    delivered_only = bool(kpis.get("business_rules", {}).get("delivered_only", True))
    enable_rowcounts = bool(kpis.get("global", {}).get("enable_rowcounts", False))

    spark = (
        SparkSession.builder
        .appName("uplift_gold_pbi_fact")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    apply_windows_bind_mount_safe_conf(spark)

    # ----------------------------
    # Read SILVER
    # ----------------------------
    fact_orders_path = os.path.join(silver_dir, "fact_orders")
    fact_items_path = os.path.join(silver_dir, "fact_order_items")
    dim_products_path = os.path.join(silver_dir, "dim_products")

    orders = spark.read.parquet(fact_orders_path)
    items = spark.read.parquet(fact_items_path)
    products = spark.read.parquet(dim_products_path)

    # ----------------------------
    # Determine affected dates/months from batch
    # ----------------------------
    orders_batch = orders.filter(F.col(batch_key) == F.lit(batch_id))

    if delivered_only and "order_status" in orders_batch.columns:
        orders_batch = orders_batch.filter(F.col("order_status") == F.lit("delivered"))

    if "order_purchase_timestamp" not in orders_batch.columns:
        raise ValueError("fact_orders must contain order_purchase_timestamp")

    orders_batch = (
        orders_batch
        .select("order_id", "order_purchase_timestamp")
        .withColumn("date", F.to_date(F.col("order_purchase_timestamp")))
        .withColumn("month", F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"))
    ).persist(StorageLevel.MEMORY_AND_DISK)

    affected_dates = [r["date"].isoformat() for r in orders_batch.select("date").distinct().collect()]
    affected_months = [r["month"] for r in orders_batch.select("month").distinct().collect()]

    if not affected_dates:
        print(f"[NO-OP] No affected dates for batch_id={batch_id} (maybe filtered out by delivered_only)")
        orders_batch.unpersist()
        spark.stop()
        return

    # ----------------------------
    # Orders all 
    # ----------------------------
    orders_all = orders
    if delivered_only and "order_status" in orders_all.columns:
        orders_all = orders_all.filter(F.col("order_status") == F.lit("delivered"))

    keep_orders_cols = [c for c in [
        "order_id",
        "order_purchase_timestamp",
        "customer_state",
        "customer_city",
        "payment_total_value",
        "payment_type_main",
    ] if c in orders_all.columns]

    orders_all = (
        orders_all
        .select(*keep_orders_cols)
        .withColumn("date", F.to_date(F.col("order_purchase_timestamp")))
        .withColumn("month", F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"))
        .withColumn("payment_total_value", F.col("payment_total_value").cast("double"))
        .withColumn(
            "payment_type",
            F.when(F.col("payment_type_main").isNull() | (F.col("payment_type_main") == ""), F.lit("unknown"))
             .otherwise(F.col("payment_type_main"))
        )
        .withColumn(
            "customer_state",
            F.when(F.col("customer_state").isNull() | (F.col("customer_state") == ""), F.lit("unknown"))
             .otherwise(F.col("customer_state"))
        )
        .withColumn(
            "customer_city",
            F.when(F.col("customer_city").isNull() | (F.col("customer_city") == ""), F.lit("unknown"))
             .otherwise(F.col("customer_city"))
        )
    )

    orders_daily = orders_all.filter(F.col("date").isin(affected_dates)).persist(StorageLevel.MEMORY_AND_DISK)
    orders_monthly = orders_all.filter(F.col("month").isin(affected_months)).persist(StorageLevel.MEMORY_AND_DISK)

    # ----------------------------
    # Items 
    # ----------------------------
    wanted_items = [c for c in ["order_id", "order_item_id", "product_id", "price", "freight_value", batch_key] if c in items.columns]
    items_use = (
        safe_cols(items, wanted_items)
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
    )

    affected_order_ids_daily = orders_daily.select("order_id").distinct()
    affected_order_ids_monthly = orders_monthly.select("order_id").distinct()

    items_daily = items_use.join(affected_order_ids_daily, on="order_id", how="inner").persist(StorageLevel.MEMORY_AND_DISK)
    items_monthly = items_use.join(affected_order_ids_monthly, on="order_id", how="inner").persist(StorageLevel.MEMORY_AND_DISK)

    # ----------------------------
    # Products dim 
    # ----------------------------
    prod_use = products.select("product_id", "product_category_name")
    prod_use = F.broadcast(prod_use)

    # ----------------------------
    # Base tables for existing KPIs
    # ----------------------------
    base_daily = (
        items_daily
        .join(
            orders_daily.select("order_id", "date", "month", "customer_state", "customer_city", "payment_type", "payment_total_value"),
            on="order_id",
            how="inner",
        )
        .join(prod_use, on="product_id", how="left")
        .withColumn(
            "product_category_name",
            F.when(F.col("product_category_name").isNull() | (F.col("product_category_name") == ""), F.lit("unknown"))
             .otherwise(F.col("product_category_name"))
        )
    ).persist(StorageLevel.MEMORY_AND_DISK)

    base_monthly = (
        items_monthly
        .join(
            orders_monthly.select("order_id", "date", "month", "customer_state", "customer_city", "payment_type", "payment_total_value"),
            on="order_id",
            how="inner",
        )
        .join(prod_use, on="product_id", how="left")
        .withColumn(
            "product_category_name",
            F.when(F.col("product_category_name").isNull() | (F.col("product_category_name") == ""), F.lit("unknown"))
             .otherwise(F.col("product_category_name"))
        )
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # Orders monthly view for payment_mix
    orders_monthly_view = orders_monthly.select("month", "payment_type", "payment_total_value", "order_id")

    # ----------------------------
    # PBI
    # ----------------------------
    item_cnt_daily = items_daily.groupBy("order_id").agg(F.count(F.lit(1)).alias("item_cnt"))
    pbi_base_daily = (
        items_daily
        .join(item_cnt_daily, on="order_id", how="inner")
        .join(
            orders_daily.select("order_id", "date", "customer_state", "payment_type", "payment_total_value"),
            on="order_id",
            how="inner",
        )
        .join(prod_use, on="product_id", how="left")
        .withColumn(
            "product_category_name",
            F.when(F.col("product_category_name").isNull() | (F.col("product_category_name") == ""), F.lit("unknown"))
             .otherwise(F.col("product_category_name"))
        )
        .withColumn(
            "payment_total_value",
            F.when(F.col("item_cnt") <= 0, F.lit(0.0)).otherwise(F.col("payment_total_value") / F.col("item_cnt"))
        )
        .select(
            "order_id",
            "date",
            "customer_state",
            "payment_type",
            "product_category_name",
            "price",
            "freight_value",
            "payment_total_value",
        )
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # ----------------------------
    # Build all configured tables
    # ----------------------------
    tables_cfg = kpis["tables"]
    rowcounts: Dict[str, int] = {}
    run_at_utc = datetime.utcnow().isoformat()

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
        elif src == "pbi_base_daily":
            src_df = pbi_base_daily
        else:
            raise ValueError(f"Unknown source: {src} (table={key})")

        agg_df = agg_from_config(src_df, group_by=group_by, metrics=metrics)
        agg_df = apply_derived_metrics(agg_df, derived)

        # Special-case: payment_mix share
        if key == "payment_mix":
            w = Window.partitionBy("month")
            agg_df = agg_df.withColumn("month_total", F.sum("total_payment_value").over(w))
            agg_df = agg_df.withColumn(
                "share",
                F.expr("CASE WHEN month_total = 0 THEN 0 ELSE total_payment_value / month_total END")
            ).drop("month_total")

        # Special-case: pbi_fact_daily (add required cols for export/publish)
        if key == "pbi_fact_daily":
            # month_date = first day of month for the 'date'
            agg_df = agg_df.withColumn("month_date", F.date_trunc("month", F.col("date")).cast("date"))

            # stable fact_id (doesn't depend on revenue etc., only grain keys)
            agg_df = agg_df.withColumn(
                "fact_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("date").cast("string"),
                        F.col("customer_state").cast("string"),
                        F.col("payment_type").cast("string"),
                        F.col("product_category_name").cast("string"),
                    ),
                    256,
                ),
            )

            agg_df = (
                agg_df
                .withColumn(batch_key, F.lit(batch_id))
                .withColumn("updated_at", F.lit(run_at_utc))
            )

        # Guard: do not silently write empty outputs
        if df_is_empty(agg_df):
            raise RuntimeError(f"[GOLD] Table '{key}' produced 0 rows for batch_id={batch_id}. Stop to avoid empty parquet outputs.")

        write_partition_overwrite(agg_df, out_path, partition_by)

        if enable_rowcounts:
            rowcounts[key] = int(agg_df.count())

    # ----------------------------
    # Meta + state
    # ----------------------------
    meta_payload = {
        "batch_id": batch_id,
        "run_at_utc": run_at_utc,
        "window_strategy": kpis["incremental"]["window_strategy"],
        "affected_dates": affected_dates,
        "affected_months": affected_months,
        "rowcounts": rowcounts if enable_rowcounts else {"_disabled": True},
    }
    write_json(os.path.join(meta_dir, f"batch_{batch_id}.json"), meta_payload)

    write_json(gold_state_path, {
        "last_processed_batch_id": batch_id,
        "updated_at": run_at_utc,
    })

    # cleanup caches
    orders_batch.unpersist()
    orders_daily.unpersist()
    orders_monthly.unpersist()
    items_daily.unpersist()
    items_monthly.unpersist()
    base_daily.unpersist()
    base_monthly.unpersist()
    pbi_base_daily.unpersist()

    print(
        f"[OK] Gold aggregate completed for batch_id={batch_id}. "
        f"affected_dates={len(affected_dates)}, affected_months={len(affected_months)}"
    )
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--settings", required=True, help="Path to configs/settings.yaml")
    p.add_argument("--kpis", required=False, default="configs/gold_kpis.yaml", help="Path to configs/gold_kpis.yaml")
    p.add_argument("--batch_id", required=True)
    args = p.parse_args()
    run(args.settings, args.kpis, args.batch_id)


if __name__ == "__main__":
    main()

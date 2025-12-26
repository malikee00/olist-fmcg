from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import List, Dict, Any

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# ----------------------------
# Utils
# ----------------------------
def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def normalize_cols(df: DataFrame) -> DataFrame:
    for c in df.columns:
        new_c = c.strip().lower().replace(" ", "_").replace("-", "_")
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df


def require_cols(df: DataFrame, cols: List[str], ctx: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{ctx}] Missing required columns: {missing}. Found={df.columns}")


def read_bronze_batch(spark: SparkSession, bronze_root: str, table: str, batch_col: str, batch_id: str) -> DataFrame:
    path = os.path.join(bronze_root, table)
    df = spark.read.parquet(path)
    df = normalize_cols(df)
    if batch_col not in df.columns:
        raise ValueError(f"[BRONZE] Missing batch_col '{batch_col}' in table={table}. cols={df.columns}")
    return df.filter(F.col(batch_col) == F.lit(batch_id))


def cast_orders(df: DataFrame) -> DataFrame:
    ts = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for c in ts:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df


def cast_items(df: DataFrame) -> DataFrame:
    if "shipping_limit_date" in df.columns:
        df = df.withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
    if "order_item_id" in df.columns:
        df = df.withColumn("order_item_id", F.col("order_item_id").cast("int"))
    for c in ["price", "freight_value"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    return df


def cast_products(df: DataFrame) -> DataFrame:
    if "product_category_name" in df.columns:
        df = df.withColumn("product_category_name", F.trim(F.col("product_category_name")))
        df = df.withColumn(
            "product_category_name",
            F.when(F.col("product_category_name").isNull() | (F.col("product_category_name") == ""), F.lit("unknown"))
             .otherwise(F.col("product_category_name"))
        )
    return df


def cast_customers(df: DataFrame) -> DataFrame:
    for c in ["customer_city", "customer_state"]:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.lower(F.col(c))))
    return df


def select_cols_strict(df: DataFrame, cols: List[str]) -> DataFrame:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[SILVER CONTRACT] Missing columns: {missing}")
    return df.select(*cols)


def upsert_union_dedup_overwrite(batch_df: DataFrame, existing_path: str, key_cols: List[str]) -> DataFrame:
    spark = batch_df.sparkSession
    if os.path.exists(existing_path) and os.listdir(existing_path):
        existing = spark.read.parquet(existing_path)
        existing = normalize_cols(existing)
        combined = existing.unionByName(batch_df, allowMissingColumns=True)
    else:
        combined = batch_df
    return combined.dropDuplicates(key_cols)


def write_partition_overwrite_by_batch(
    spark: SparkSession,
    df_batch: DataFrame,
    out_path: str,
    partition_by: List[str],
) -> None:
    """
    Idempotent facts strategy:
    - facts are written partitioned by batch_id
    - on rerun for same batch_id, only that partition is overwritten (dynamic partition overwrite)
    """
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        df_batch.write
        .mode("overwrite")
        .partitionBy(*partition_by)
        .parquet(out_path)
    )


def write_meta(silver_root: str, batch_id: str, payload: Dict[str, Any]) -> None:
    meta_dir = os.path.join(silver_root, "_meta")
    ensure_dir(meta_dir)
    path = os.path.join(meta_dir, f"batch_{batch_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ----------------------------
# Main
# ----------------------------
def run(config_path: str, batch_id: str) -> None:
    cfg = read_yaml(config_path)
    g = cfg["global"]
    bronze_root = g["bronze_root"]
    silver_root = g["silver_root"]
    batch_col = g.get("batch_col", "batch_id")

    spark = SparkSession.builder.appName("uplift_phase3_silver_transform").getOrCreate()

    # Read bronze batch
    orders_b = read_bronze_batch(spark, bronze_root, cfg["tables"]["orders"]["bronze_name"], batch_col, batch_id)
    items_b = read_bronze_batch(spark, bronze_root, cfg["tables"]["order_items"]["bronze_name"], batch_col, batch_id)
    products_b = read_bronze_batch(spark, bronze_root, cfg["tables"]["products"]["bronze_name"], batch_col, batch_id)
    customers_b = read_bronze_batch(spark, bronze_root, cfg["tables"]["customers"]["bronze_name"], batch_col, batch_id)

    # Require minimal cols
    require_cols(orders_b, cfg["tables"]["orders"]["required_cols"] + [batch_col], "orders_bronze")
    require_cols(items_b, cfg["tables"]["order_items"]["required_cols"] + [batch_col], "items_bronze")
    require_cols(products_b, cfg["tables"]["products"]["required_cols"] + [batch_col], "products_bronze")
    require_cols(customers_b, cfg["tables"]["customers"]["required_cols"] + [batch_col], "customers_bronze")

    # Cast / standardize
    orders_b = cast_orders(orders_b)
    items_b = cast_items(items_b)
    products_b = cast_products(products_b)
    customers_b = cast_customers(customers_b)

    # Dedup within batch (facts grain)
    orders_b = orders_b.dropDuplicates(["order_id"])
    items_b = items_b.dropDuplicates(["order_id", "order_item_id"])

    # Build dims 
    dim_customers_cfg = cfg["silver"]["dims"]["dim_customers"]
    dim_products_cfg = cfg["silver"]["dims"]["dim_products"]

    dim_customers_batch = select_cols_strict(customers_b, dim_customers_cfg["columns"]).dropDuplicates(dim_customers_cfg["key"])
    dim_products_batch = select_cols_strict(products_b, dim_products_cfg["columns"]).dropDuplicates(dim_products_cfg["key"])

    # Upsert dims 
    dim_customers_out = os.path.join(silver_root, dim_customers_cfg["path"])
    dim_products_out = os.path.join(silver_root, dim_products_cfg["path"])

    ensure_dir(silver_root)

    dim_customers_final = upsert_union_dedup_overwrite(dim_customers_batch, dim_customers_out, dim_customers_cfg["key"])
    dim_products_final = upsert_union_dedup_overwrite(dim_products_batch, dim_products_out, dim_products_cfg["key"])

    dim_customers_final.write.mode("overwrite").parquet(dim_customers_out)
    dim_products_final.write.mode("overwrite").parquet(dim_products_out)

    # Build facts 
    fact_orders_cfg = cfg["silver"]["facts"]["fact_orders"]
    fact_items_cfg = cfg["silver"]["facts"]["fact_order_items"]

    fact_orders_batch = select_cols_strict(orders_b, fact_orders_cfg["columns"])
    fact_items_joined = items_b.join(
        dim_products_batch.select("product_id", "product_category_name"),
        on="product_id",
        how="left"
    )
    fact_items_batch = select_cols_strict(fact_items_joined, fact_items_cfg["columns"])

    # Write facts with partition overwrite (idempotent per batch)
    fact_orders_out = os.path.join(silver_root, fact_orders_cfg["path"])
    fact_items_out = os.path.join(silver_root, fact_items_cfg["path"])

    write_partition_overwrite_by_batch(spark, fact_orders_batch, fact_orders_out, fact_orders_cfg["partition_by"])
    write_partition_overwrite_by_batch(spark, fact_items_batch, fact_items_out, fact_items_cfg["partition_by"])

    # Optional meta log
    meta = {
        "batch_id": batch_id,
        "run_at": datetime.utcnow().isoformat(),
        "rows": {
            "dim_customers_batch": dim_customers_batch.count(),
            "dim_products_batch": dim_products_batch.count(),
            "fact_orders_batch": fact_orders_batch.count(),
            "fact_order_items_batch": fact_items_batch.count(),
        }
    }
    write_meta(silver_root, batch_id, meta)

    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--batch_id", required=True)
    args = p.parse_args()
    run(args.config, args.batch_id)


if __name__ == "__main__":
    main()

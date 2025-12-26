from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Dict, Any, List

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# =====================================================
# IO helpers
# =====================================================
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


def read_bronze_batch(
    spark: SparkSession,
    input_dir: str,
    table: str,
    batch_key: str,
    batch_id: str
) -> DataFrame:
    path = os.path.join(input_dir, table)
    df = spark.read.parquet(path)
    df = normalize_cols(df)
    if batch_key not in df.columns:
        raise ValueError(f"[BRONZE] Missing batch_key '{batch_key}' in table={table}. cols={df.columns}")
    return df.filter(F.col(batch_key) == F.lit(batch_id))


# =====================================================
# Transform primitives
# =====================================================
def apply_cast_timestamps(df: DataFrame, cols: List[str]) -> DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df


def apply_cast_numeric(df: DataFrame, cast_map: Dict[str, List[str]]) -> DataFrame:
    for dtype, cols in cast_map.items():
        for c in cols:
            if c in df.columns:
                df = df.withColumn(c, F.col(c).cast(dtype))
    return df


def apply_fill_unknown(df: DataFrame, cols: List[str]) -> DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull() | (F.col(c) == ""), F.lit("unknown"))
                 .otherwise(F.col(c))
            )
    return df


def apply_lowercase_trim(df: DataFrame, cols: List[str]) -> DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.lower(F.col(c))))
    return df


def select_cols_strict(df: DataFrame, cols: List[str]) -> DataFrame:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[SILVER CONTRACT] Missing columns: {missing}")
    return df.select(*cols)


# =====================================================
# SAFE JOIN (FIX AMBIGUOUS_REFERENCE)
# =====================================================
def safe_join(
    left_df: DataFrame,
    right_df: DataFrame,
    how: str,
    left_keys: List[str],
    right_keys: List[str]
) -> DataFrame:
    """
    Join helper to avoid duplicate / ambiguous columns.
    - If left_keys == right_keys: use join(on=keys)
    - Else: join by condition, then drop right key columns
    """
    if left_keys == right_keys:
        return left_df.join(right_df, on=left_keys, how=how)

    cond = [left_df[lk] == right_df[rk] for lk, rk in zip(left_keys, right_keys)]
    joined = left_df.join(right_df, on=cond, how=how)

    for rk in right_keys:
        if rk in joined.columns:
            joined = joined.drop(right_df[rk])

    return joined


# =====================================================
# Incremental write strategies
# =====================================================
def upsert_union_dedup_overwrite(batch_df: DataFrame, out_path: str, key_cols: List[str]) -> None:
    spark = batch_df.sparkSession
    if os.path.exists(out_path) and os.listdir(out_path):
        existing = spark.read.parquet(out_path)
        existing = normalize_cols(existing)
        combined = existing.unionByName(batch_df, allowMissingColumns=True)
    else:
        combined = batch_df

    final_df = combined.dropDuplicates(key_cols)
    final_df.write.mode("overwrite").parquet(out_path)


def write_fact_idempotent_by_batch(
    spark: SparkSession,
    df_batch: DataFrame,
    out_path: str,
    partition_by: List[str]
) -> None:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        df_batch.write
        .mode("overwrite")
        .partitionBy(*partition_by)
        .parquet(out_path)
    )


def write_meta(output_dir: str, batch_id: str, payload: Dict[str, Any]) -> None:
    meta_dir = os.path.join(output_dir, "_meta")
    ensure_dir(meta_dir)
    path = os.path.join(meta_dir, f"batch_{batch_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# =====================================================
# Virtual source: payments_agg
# =====================================================
def build_payments_agg(payments_b: DataFrame) -> DataFrame:
    agg_base = (
        payments_b
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("payment_total_value"),
            F.count(F.lit(1)).alias("payment_count")
        )
    )

    type_counts = (
        payments_b
        .withColumn("payment_type", F.trim(F.lower(F.col("payment_type"))))
        .groupBy("order_id", "payment_type")
        .count()
    )

    max_counts = (
        type_counts
        .groupBy("order_id")
        .agg(F.max("count").alias("max_cnt"))
    )

    main_type = (
        type_counts
        .join(max_counts, on="order_id", how="inner")
        .filter(F.col("count") == F.col("max_cnt"))
        .groupBy("order_id")
        .agg(F.first("payment_type").alias("payment_type_main"))
    )

    return agg_base.join(main_type, on="order_id", how="left")


# =====================================================
# MAIN
# =====================================================
def run(config_path: str, batch_id: str) -> None:
    cfg = read_yaml(config_path)
    g = cfg["global"]

    input_dir = g["input_dir"]
    output_dir = g["output_dir"]
    batch_key = g.get("batch_key", "batch_id")

    spark = SparkSession.builder.appName(
        "uplift_phase3_silver_transform_config_driven"
    ).getOrCreate()

    bronze_sources: Dict[str, DataFrame] = {}
    for src_name, src_cfg in cfg["sources"].items():
        df = read_bronze_batch(
            spark,
            input_dir,
            src_cfg["table"],
            batch_key,
            batch_id
        )
        require_cols(df, src_cfg["required_cols"], f"bronze_source:{src_name}")
        bronze_sources[src_name] = df

    outputs_cfg = cfg["outputs"]

    # =================================================
    # DIM: customers
    # =================================================
    dim_c_cfg = outputs_cfg["dim_customers"]
    customers_b = bronze_sources["customers"]
    std = dim_c_cfg.get("transform", {}).get("standardize", {})
    customers_b = apply_lowercase_trim(customers_b, std.get("lowercase_trim_cols", []))

    dim_customers_batch = (
        select_cols_strict(customers_b, dim_c_cfg["select_columns"])
        .dropDuplicates(dim_c_cfg["primary_key"])
    )

    dim_customers_out = os.path.join(output_dir, dim_c_cfg["path"])
    ensure_dir(output_dir)
    upsert_union_dedup_overwrite(
        dim_customers_batch,
        dim_customers_out,
        dim_c_cfg["primary_key"]
    )

    # =================================================
    # DIM: products
    # =================================================
    dim_p_cfg = outputs_cfg["dim_products"]
    products_b = bronze_sources["products"]
    products_b = apply_fill_unknown(
        products_b,
        dim_p_cfg.get("transform", {}).get("fill_unknown_cols", [])
    )

    dim_products_batch = (
        select_cols_strict(products_b, dim_p_cfg["select_columns"])
        .dropDuplicates(dim_p_cfg["primary_key"])
    )

    dim_products_out = os.path.join(output_dir, dim_p_cfg["path"])
    upsert_union_dedup_overwrite(
        dim_products_batch,
        dim_products_out,
        dim_p_cfg["primary_key"]
    )

    # =================================================
    # Virtual source
    # =================================================
    payments_agg_df = build_payments_agg(bronze_sources["payments"])

    # =================================================
    # FACT: orders
    # =================================================
    fo_cfg = outputs_cfg["fact_orders"]
    orders_b = bronze_sources["orders"]
    orders_b = apply_cast_timestamps(
        orders_b,
        fo_cfg.get("transform", {}).get("cast_timestamps", [])
    )
    orders_b = orders_b.dropDuplicates(fo_cfg["primary_key"])

    fact_orders_df = orders_b
    for j in fo_cfg.get("joins", []):
        right_src = j["right"]["source"]
        left_keys = j["left"]["keys"]
        right_keys = j["right"]["keys"]

        if right_src == "customers":
            right_df = dim_customers_batch
        elif right_src == "payments_agg":
            right_df = payments_agg_df
        else:
            raise ValueError(f"Unknown join right source: {right_src}")

        select_right = j.get("select_from_right", [])
        if select_right:
            right_df = right_df.select(*(right_keys + select_right))

        fact_orders_df = safe_join(
            left_df=fact_orders_df,
            right_df=right_df,
            how="left",
            left_keys=left_keys,
            right_keys=right_keys
        )

    fact_orders_df = select_cols_strict(
        fact_orders_df,
        fo_cfg["select_columns"]
    )

    fact_orders_out = os.path.join(output_dir, fo_cfg["path"])
    write_fact_idempotent_by_batch(
        spark,
        fact_orders_df,
        fact_orders_out,
        fo_cfg["partition_by"]
    )

    # =================================================
    # FACT: order_items
    # =================================================
    fi_cfg = outputs_cfg["fact_order_items"]
    items_b = bronze_sources["order_items"]

    tcfg = fi_cfg.get("transform", {})
    items_b = apply_cast_timestamps(items_b, tcfg.get("cast_timestamps", []))
    items_b = apply_cast_numeric(items_b, tcfg.get("cast_numeric", {}))
    items_b = items_b.dropDuplicates(fi_cfg["primary_key"])

    fact_items_df = items_b
    for j in fi_cfg.get("joins", []):
        right_src = j["right"]["source"]
        left_keys = j["left"]["keys"]
        right_keys = j["right"]["keys"]

        if right_src == "products":
            right_df = dim_products_batch
        else:
            raise ValueError(f"Unknown join right source: {right_src}")

        select_right = j.get("select_from_right", [])
        if select_right:
            right_df = right_df.select(*(right_keys + select_right))

        fact_items_df = safe_join(
            left_df=fact_items_df,
            right_df=right_df,
            how="left",
            left_keys=left_keys,
            right_keys=right_keys
        )

    fact_items_df = select_cols_strict(
        fact_items_df,
        fi_cfg["select_columns"]
    )

    fact_items_out = os.path.join(output_dir, fi_cfg["path"])
    write_fact_idempotent_by_batch(
        spark,
        fact_items_df,
        fact_items_out,
        fi_cfg["partition_by"]
    )

    # =================================================
    # META
    # =================================================
    meta = {
        "batch_id": batch_id,
        "run_at_utc": datetime.utcnow().isoformat(),
        "rows": {
            "dim_customers_batch": dim_customers_batch.count(),
            "dim_products_batch": dim_products_batch.count(),
            "fact_orders_batch": fact_orders_df.count(),
            "fact_order_items_batch": fact_items_df.count(),
        }
    }
    write_meta(output_dir, batch_id, meta)

    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--batch_id", required=True)
    args = p.parse_args()
    run(args.config, args.batch_id)


if __name__ == "__main__":
    main()

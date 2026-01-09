# spark_jobs/gold_validate.py
from __future__ import annotations

import argparse
import json
import os
from glob import glob
from typing import Dict, Any, List

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def apply_spark_hardening(spark: SparkSession) -> None:
    try:
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        hconf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        hconf.set("parquet.enable.summary-metadata", "false")
        hconf.set("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
    except Exception:
        pass


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def safe_count(df) -> int:
    return int(df.count())


def load_meta(meta_path: str) -> Dict[str, Any]:
    if not os.path.exists(meta_path):
        fail(f"[GOLD] Missing meta file: {meta_path}")
    with open(meta_path, "r", encoding="utf-8") as f:
        return json.load(f)


def parquet_has_files(path: str) -> bool:
    # prevent UNABLE_TO_INFER_SCHEMA when directory exists but empty
    if not os.path.exists(path):
        return False
    # parquet part files are typically in partitions/subfolders; recurse
    matches = glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    return len(matches) > 0


def run(settings_path: str, kpis_path: str, batch_id: str) -> None:
    settings = read_yaml(settings_path)
    kpis = read_yaml(kpis_path)

    gold_dir = settings["paths"]["gold_dir"]
    quality = kpis.get("quality", {})
    min_rows = int(quality.get("min_rows_per_partition", 1))
    forbid_negative = bool(quality.get("forbid_negative", True))
    enforce_uniqueness = bool(quality.get("enforce_uniqueness", True))

    meta_path = os.path.join(gold_dir, "_meta", f"batch_{batch_id}.json")
    meta = load_meta(meta_path)

    affected_dates: List[str] = meta.get("affected_dates", [])
    affected_months: List[str] = meta.get("affected_months", [])

    spark = SparkSession.builder.appName("uplift_gold_validate").getOrCreate()
    apply_spark_hardening(spark)

    tables_cfg = kpis["tables"]

    def path_of(table_key: str) -> str:
        return os.path.join(gold_dir, tables_cfg[table_key]["path"])

    def check_parquet_nonempty(p: str, table_key: str) -> None:
        if not parquet_has_files(p):
            fail(f"[GOLD] Missing/empty parquet for table '{table_key}': {p}")

    # ----------------------------
    # Existing validations
    # ----------------------------
    # kpi_daily
    if "kpi_daily" in tables_cfg:
        kd_path = path_of("kpi_daily")
        check_parquet_nonempty(kd_path, "kpi_daily")
        kd = spark.read.parquet(kd_path).filter(F.col("date").isin(affected_dates))
        if safe_count(kd) < min_rows:
            fail("[GOLD][AFFECTED] kpi_daily empty for affected dates")
        if forbid_negative and safe_count(kd.filter((F.col("revenue") < 0) | (F.col("orders") < 0) | (F.col("aov") < 0))) > 0:
            fail("[GOLD][AFFECTED] negative values in kpi_daily")
        if enforce_uniqueness and safe_count(kd.groupBy("date").count().filter(F.col("count") > 1)) > 0:
            fail("[GOLD][AFFECTED] duplicate date in kpi_daily")

    # kpi_monthly
    if "kpi_monthly" in tables_cfg:
        km_path = path_of("kpi_monthly")
        check_parquet_nonempty(km_path, "kpi_monthly")
        km = spark.read.parquet(km_path).filter(F.col("month").isin(affected_months))
        if safe_count(km) < min_rows:
            fail("[GOLD][AFFECTED] kpi_monthly empty for affected months")
        if forbid_negative and safe_count(km.filter((F.col("revenue") < 0) | (F.col("orders") < 0) | (F.col("aov") < 0))) > 0:
            fail("[GOLD][AFFECTED] negative values in kpi_monthly")
        if enforce_uniqueness and safe_count(km.groupBy("month").count().filter(F.col("count") > 1)) > 0:
            fail("[GOLD][AFFECTED] duplicate month in kpi_monthly")

    # top_categories (path is top_products in your yaml)
    if "top_categories" in tables_cfg:
        tp_path = path_of("top_categories")
        check_parquet_nonempty(tp_path, "top_categories")
        tp = spark.read.parquet(tp_path).filter(F.col("month").isin(affected_months))
        if safe_count(tp) < min_rows:
            fail("[GOLD][AFFECTED] top_categories empty for affected months")
        if forbid_negative and safe_count(tp.filter((F.col("revenue") < 0) | (F.col("orders") < 0))) > 0:
            fail("[GOLD][AFFECTED] negative values in top_categories")
        if enforce_uniqueness and safe_count(tp.groupBy("month", "product_category_name").count().filter(F.col("count") > 1)) > 0:
            fail("[GOLD][AFFECTED] duplicate (month, product_category_name) in top_categories")

    # payment_mix
    if "payment_mix" in tables_cfg:
        pm_path = path_of("payment_mix")
        check_parquet_nonempty(pm_path, "payment_mix")
        pm = spark.read.parquet(pm_path).filter(F.col("month").isin(affected_months))
        if safe_count(pm) < min_rows:
            fail("[GOLD][AFFECTED] payment_mix empty for affected months")
        # share exists from special-case
        if forbid_negative and safe_count(pm.filter((F.col("total_payment_value") < 0) | (F.col("share") < 0))) > 0:
            fail("[GOLD][AFFECTED] negative values in payment_mix")
        if enforce_uniqueness and safe_count(pm.groupBy("month", "payment_type").count().filter(F.col("count") > 1)) > 0:
            fail("[GOLD][AFFECTED] duplicate (month, payment_type) in payment_mix")

    # kpi_by_state
    if "kpi_by_state" in tables_cfg and tables_cfg["kpi_by_state"].get("enabled", True):
        ks_path = path_of("kpi_by_state")
        check_parquet_nonempty(ks_path, "kpi_by_state")
        ks = spark.read.parquet(ks_path).filter(F.col("month").isin(affected_months))
        if safe_count(ks) < min_rows:
            fail("[GOLD][AFFECTED] kpi_by_state empty for affected months")
        if forbid_negative and safe_count(ks.filter((F.col("revenue") < 0) | (F.col("orders") < 0))) > 0:
            fail("[GOLD][AFFECTED] negative values in kpi_by_state")
        if enforce_uniqueness and safe_count(ks.groupBy("month", "customer_state").count().filter(F.col("count") > 1)) > 0:
            fail("[GOLD][AFFECTED] duplicate (month, customer_state) in kpi_by_state")

    # ----------------------------
    # NEW: pbi_fact_daily validation
    # ----------------------------
    if "pbi_fact_daily" in tables_cfg:
        pf_path = path_of("pbi_fact_daily")
        check_parquet_nonempty(pf_path, "pbi_fact_daily")
        pf = spark.read.parquet(pf_path).filter(F.col("date").isin(affected_dates))

        if safe_count(pf) < min_rows:
            fail("[GOLD][AFFECTED] pbi_fact_daily empty for affected dates")

        # Required columns sanity (fail early if schema drift)
        required_cols = [
            "date", "customer_state", "payment_type", "product_category_name",
            "revenue", "orders", "total_payment_value", "avg_price", "avg_freight",
        ]
        missing = [c for c in required_cols if c not in pf.columns]
        if missing:
            fail(f"[GOLD][SCHEMA] pbi_fact_daily missing cols: {missing}. Found={pf.columns}")

        if forbid_negative:
            bad = pf.filter(
                (F.col("revenue") < 0)
                | (F.col("orders") < 0)
                | (F.col("total_payment_value") < 0)
                | (F.col("avg_price") < 0)
                | (F.col("avg_freight") < 0)
            )
            if safe_count(bad) > 0:
                fail("[GOLD][AFFECTED] negative values in pbi_fact_daily")

        if enforce_uniqueness:
            dup = (
                pf.groupBy("date", "customer_state", "payment_type", "product_category_name")
                .count()
                .filter(F.col("count") > 1)
            )
            if safe_count(dup) > 0:
                fail("[GOLD][AFFECTED] duplicate grain in pbi_fact_daily")

    print(f"[PASS] Gold validation OK for batch={batch_id}. affected_dates={len(affected_dates)}, affected_months={len(affected_months)}")
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

from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Any

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

    spark.conf.set(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
    )
    spark.conf.set(
        "spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter",
    )


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def safe_count(df) -> int:
    return int(df.count())


def load_meta(meta_path: str) -> Dict[str, Any]:
    if not os.path.exists(meta_path):
        fail(f"[GOLD] Missing meta file: {meta_path}")
    with open(meta_path, "r", encoding="utf-8") as f:
        return json.load(f)


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

    affected_dates = meta.get("affected_dates", [])
    affected_months = meta.get("affected_months", [])

    spark = SparkSession.builder.appName("uplift_phase4_gold_validate").getOrCreate()
    apply_spark_hardening(spark)

    tables_cfg = kpis["tables"]

    def path_of(table_key: str) -> str:
        return os.path.join(gold_dir, tables_cfg[table_key]["path"])

    def check_exists(p: str) -> None:
        if not os.path.exists(p):
            fail(f"[GOLD] Missing table path: {p}")

    # kpi_daily
    kd_path = path_of("kpi_daily"); check_exists(kd_path)
    kd = spark.read.parquet(kd_path).filter(F.col("date").isin(affected_dates))
    if safe_count(kd) < min_rows:
        fail("[GOLD][AFFECTED] kpi_daily empty for affected dates")
    if forbid_negative and safe_count(kd.filter((F.col("revenue") < 0) | (F.col("orders") < 0) | (F.col("aov") < 0))) > 0:
        fail("[GOLD][AFFECTED] negative values in kpi_daily")
    if enforce_uniqueness and safe_count(kd.groupBy("date").count().filter(F.col("count") > 1)) > 0:
        fail("[GOLD][AFFECTED] duplicate date in kpi_daily")

    # kpi_monthly
    km_path = path_of("kpi_monthly"); check_exists(km_path)
    km = spark.read.parquet(km_path).filter(F.col("month").isin(affected_months))
    if safe_count(km) < min_rows:
        fail("[GOLD][AFFECTED] kpi_monthly empty for affected months")
    if forbid_negative and safe_count(km.filter((F.col("revenue") < 0) | (F.col("orders") < 0) | (F.col("aov") < 0))) > 0:
        fail("[GOLD][AFFECTED] negative values in kpi_monthly")
    if enforce_uniqueness and safe_count(km.groupBy("month").count().filter(F.col("count") > 1)) > 0:
        fail("[GOLD][AFFECTED] duplicate month in kpi_monthly")

    # top_categories
    tp_key = "top_categories"
    tp_path = os.path.join(gold_dir, tables_cfg[tp_key]["path"]); check_exists(tp_path)
    tp = spark.read.parquet(tp_path).filter(F.col("month").isin(affected_months))
    if safe_count(tp) < min_rows:
        fail("[GOLD][AFFECTED] top_categories empty for affected months")
    if forbid_negative and safe_count(tp.filter((F.col("revenue") < 0) | (F.col("orders") < 0))) > 0:
        fail("[GOLD][AFFECTED] negative values in top_categories")
    if enforce_uniqueness and safe_count(tp.groupBy("month", "product_category_name").count().filter(F.col("count") > 1)) > 0:
        fail("[GOLD][AFFECTED] duplicate (month, product_category_name) in top_categories")

    # payment_mix
    pm_path = path_of("payment_mix"); check_exists(pm_path)
    pm = spark.read.parquet(pm_path).filter(F.col("month").isin(affected_months))
    if safe_count(pm) < min_rows:
        fail("[GOLD][AFFECTED] payment_mix empty for affected months")
    if forbid_negative and safe_count(pm.filter((F.col("total_payment_value") < 0) | (F.col("share") < 0))) > 0:
        fail("[GOLD][AFFECTED] negative values in payment_mix")
    if enforce_uniqueness and safe_count(pm.groupBy("month", "payment_type").count().filter(F.col("count") > 1)) > 0:
        fail("[GOLD][AFFECTED] duplicate (month, payment_type) in payment_mix")

    # kpi_by_state
    ks_cfg = tables_cfg.get("kpi_by_state", {})
    if ks_cfg and ks_cfg.get("enabled", True):
        ks_path = os.path.join(gold_dir, ks_cfg["path"]); check_exists(ks_path)
        ks = spark.read.parquet(ks_path).filter(F.col("month").isin(affected_months))
        if safe_count(ks) < min_rows:
            fail("[GOLD][AFFECTED] kpi_by_state empty for affected months")
        if forbid_negative and safe_count(ks.filter((F.col("revenue") < 0) | (F.col("orders") < 0))) > 0:
            fail("[GOLD][AFFECTED] negative values in kpi_by_state")
        if enforce_uniqueness and safe_count(ks.groupBy("month", "customer_state").count().filter(F.col("count") > 1)) > 0:
            fail("[GOLD][AFFECTED] duplicate (month, customer_state) in kpi_by_state")

    if safe_count(spark.read.parquet(kd_path)) <= 0: fail("[GOLD][GLOBAL] kpi_daily empty total")
    if safe_count(spark.read.parquet(km_path)) <= 0: fail("[GOLD][GLOBAL] kpi_monthly empty total")
    if safe_count(spark.read.parquet(tp_path)) <= 0: fail("[GOLD][GLOBAL] top_categories empty total")
    if safe_count(spark.read.parquet(pm_path)) <= 0: fail("[GOLD][GLOBAL] payment_mix empty total")

    print(f"[PASS] Gold validation OK for batch={batch_id}. affected_dates={len(affected_dates)}, affected_months={len(affected_months)}")
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

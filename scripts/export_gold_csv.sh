#!/usr/bin/env bash
set -euo pipefail

echo "[INFO] Exporting Gold Parquet → CSV (pandas, schema-enforced)"

: "${PROJECT_ROOT:?PROJECT_ROOT not set}"

GOLD_DIR="${PROJECT_ROOT}/data/gold/olist_parquet"
EXPORT_DIR="${PROJECT_ROOT}/data/gold_exports"

mkdir -p "${EXPORT_DIR}"

PYTHON_BIN="${PYTHON_BIN:-python3}"

"${PYTHON_BIN}" - <<'PY'
import os
from pathlib import Path
import pandas as pd

project_root = Path(os.environ["PROJECT_ROOT"])
gold_dir = project_root / "data" / "gold" / "olist_parquet"
export_dir = project_root / "data" / "gold_exports"
export_dir.mkdir(parents=True, exist_ok=True)

def ensure_cols(df: pd.DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"[ERROR] {name}: missing columns {missing}. Found={list(df.columns)}")
    return df[cols].copy()

def to_date_iso(series: pd.Series) -> pd.Series:
    s = series
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.date.astype("string")
    if pd.api.types.is_object_dtype(s):
        parsed = pd.to_datetime(s, errors="coerce", utc=False)
        if parsed.notna().any():
            return parsed.dt.date.astype("string")
        # already string date
        return s.astype("string")
    if pd.api.types.is_numeric_dtype(s):
        # best effort: treat as epoch days (rare), else fail loudly
        parsed = pd.to_datetime(s, unit="D", origin="unix", errors="coerce")
        if parsed.isna().all():
            raise SystemExit("[ERROR] date column numeric but cannot parse")
        return parsed.dt.date.astype("string")
    parsed = pd.to_datetime(s, errors="coerce")
    if parsed.isna().all():
        raise SystemExit("[ERROR] cannot parse date column to ISO")
    return parsed.dt.date.astype("string")

def to_month_ym(series: pd.Series) -> pd.Series:
    s = series
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.strftime("%Y-%m").astype("string")
    if pd.api.types.is_object_dtype(s):
        # if already 'YYYY-MM', keep it
        parsed = pd.to_datetime(s, errors="coerce")
        if parsed.notna().any():
            return parsed.dt.strftime("%Y-%m").astype("string")
        return s.astype("string")
    if pd.api.types.is_numeric_dtype(s):
        parsed = pd.to_datetime(s, unit="D", origin="unix", errors="coerce")
        if parsed.isna().all():
            raise SystemExit("[ERROR] month column numeric but cannot parse")
        return parsed.dt.strftime("%Y-%m").astype("string")
    return s.astype("string")

def to_timestamp_iso(series: pd.Series) -> pd.Series:
    # export as ISO string without timezone requirement (safe for Postgres COPY as text→timestamp/timestamptz)
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    return parsed.dt.strftime("%Y-%m-%d %H:%M:%S").astype("string")

def coerce_numeric(df: pd.DataFrame, float_cols: list[str], int_cols: list[str]):
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
    return df

def coerce_string(df: pd.DataFrame, str_cols: list[str]):
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].astype("string").fillna("unknown")
    return df

def read_parquet_any(path: Path) -> pd.DataFrame:
    # Spark parquet may be a folder with part-*.parquet files.
    # pandas.read_parquet supports folder reads with pyarrow.
    return pd.read_parquet(path)

SCHEMAS = {
    "kpi_daily": {
        "path": gold_dir / "kpi_daily",
        "cols": ["date", "revenue", "orders", "aov"],
        "date_cols": ["date"],
        "month_cols": [],
        "float_cols": ["revenue", "aov"],
        "int_cols": ["orders"],
    },
    "kpi_monthly": {
        "path": gold_dir / "kpi_monthly",
        "cols": ["month", "revenue", "orders", "aov"],
        "date_cols": [],
        "month_cols": ["month"],
        "float_cols": ["revenue", "aov"],
        "int_cols": ["orders"],
    },
    "kpi_by_state": {
        "path": gold_dir / "kpi_by_state",
        "cols": ["month", "customer_state", "revenue", "orders"],
        "date_cols": [],
        "month_cols": ["month"],
        "float_cols": ["revenue"],
        "int_cols": ["orders"],
        "str_cols": ["customer_state"],
    },
    "payment_mix": {
        "path": gold_dir / "payment_mix",
        "cols": ["month", "payment_type", "total_payment_value", "share"],
        "date_cols": [],
        "month_cols": ["month"],
        "float_cols": ["total_payment_value", "share"],
        "str_cols": ["payment_type"],
    },
    "top_categories": {
        "path": gold_dir / "top_products",
        "cols": ["month", "product_category_name", "revenue", "orders", "avg_price", "avg_freight"],
        "date_cols": [],
        "month_cols": ["month"],
        "float_cols": ["revenue", "avg_price", "avg_freight"],
        "int_cols": ["orders"],
        "str_cols": ["product_category_name"],
    },

    # =========================
    # NEW: unified fact for Power BI
    # =========================
    "pbi_fact_daily": {
        "path": gold_dir / "pbi_fact_daily",
        "cols": [
            "fact_id",
            "date",
            "month_date",
            "customer_state",
            "payment_type",
            "product_category_name",
            "revenue",
            "orders",
            "total_payment_value",
            "avg_price",
            "avg_freight",
            "batch_id",
            "updated_at",
        ],
        "date_cols": ["date", "month_date"],
        "timestamp_cols": ["updated_at"],
        "month_cols": [],
        "float_cols": ["revenue", "total_payment_value", "avg_price", "avg_freight"],
        "int_cols": ["orders"],
        "str_cols": ["fact_id", "customer_state", "payment_type", "product_category_name", "batch_id"],
    },
}

for name, cfg in SCHEMAS.items():
    path = cfg["path"]
    if not path.exists():
        raise SystemExit(f"[ERROR] Path not found: {path}")

    df = read_parquet_any(path)
    df = ensure_cols(df, cfg["cols"], name)

    for c in cfg.get("date_cols", []):
        df[c] = to_date_iso(df[c])

    for c in cfg.get("timestamp_cols", []):
        df[c] = to_timestamp_iso(df[c])

    for c in cfg.get("month_cols", []):
        df[c] = to_month_ym(df[c])

    df = coerce_numeric(df, cfg.get("float_cols", []), cfg.get("int_cols", []))
    df = coerce_string(df, cfg.get("str_cols", []))

    out = export_dir / f"{name}.csv"
    df.to_csv(out, index=False)

    if "date" in df.columns:
        print(f"[CHECK] {name}.date sample:", df["date"].dropna().head(3).tolist())
    if "month" in df.columns:
        print(f"[CHECK] {name}.month sample:", df["month"].dropna().head(3).tolist())
    if "month_date" in df.columns:
        print(f"[CHECK] {name}.month_date sample:", df["month_date"].dropna().head(3).tolist())

    print(f"[OK] {name}: rows={len(df)} → {out}")

print("[DONE] Export Gold CSV finished")
PY

echo "[INFO] Export dir:"
ls -lh "${EXPORT_DIR}"

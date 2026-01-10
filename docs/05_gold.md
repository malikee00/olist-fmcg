# 🥇 Gold Layer — Business KPIs & BI Derivatives

The Gold layer represents the **business logic layer** of the Olist Analytics pipeline.

This is where analytical data is transformed into **business-ready metrics**.

---

## Purpose of the Gold Layer

The Gold layer is designed to:

- Aggregate Silver data into KPIs
- Produce datasets ready for BI consumption
- Standardize business definitions
- Support reporting and dashboards

---

## Input Source

- Silver fact and dimension tables
- Incremental batch-based processing

---

## Core Outputs

The Gold layer produces:

- `kpi_daily`
- `kpi_monthly`
- `kpi_by_state`
- `payment_mix`
- `top_products`
- `pbi_fact_daily` (BI-only derivative)

---

## BI Derivative Table

`pbi_fact_daily` is created specifically to:

- Support Power BI consumption
- Simplify BI modeling
- Reduce complex joins in the BI layer

It is **not treated as a core KPI table**.

---

## Processing Characteristics

Key characteristics:

- Aggregation-focused logic
- Business rules applied
- Batch-level overwrite or recompute
- Deterministic and repeatable outputs

---

## Output Storage

Gold outputs are stored as:

- Parquet files
- Exported as CSV for database publishing
- Stored under: data/gold/olist_parquet/


---

## Design Principles

- Business definitions are centralized
- BI consumption is simplified
- Changes are absorbed upstream
- Downstream dashboards remain stable

---

## Summary

The Gold layer translates analytical data into business insights while maintaining a clean separation from BI presentation logic.

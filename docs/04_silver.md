# 🥈 Silver Layer — Analytic-Ready Data

The Silver layer transforms raw Bronze data into **clean, structured datasets** that are ready for analytics.

This layer focuses on **data correctness, relationships, and usability**.

---

## Purpose of the Silver Layer

The Silver layer is responsible for:

- Cleaning and standardizing data
- Defining consistent grains
- Building fact and dimension tables
- Preparing data for aggregation

This layer separates **raw ingestion** from **business logic**.

---

## Input Source

- Bronze Parquet datasets
- Only newly ingested batches are processed
- Batch-aware and incremental execution

---

## Core Outputs

The Silver layer produces the following datasets:

- `fact_orders`
- `fact_order_items`
- `dim_customers`
- `dim_products`

Each dataset has a clearly defined grain.

---

## Processing Characteristics

Key characteristics:

- Incremental transformations
- Deterministic joins
- Consistent primary keys
- Referential integrity between facts and dimensions

The Silver layer avoids KPI calculations.

---

## Storage Format

- Parquet format
- Stored under: data/silver/olist_parquet/
- Metadata and batch logs stored alongside data

---

## Why No BI Logic Here?

The Silver layer is optimized for:

- Reusability
- Analytical flexibility
- Downstream aggregation

BI-specific logic is intentionally excluded.

---

## Design Principles

- Clear separation of concerns
- Stable schemas
- Reusable datasets
- Analytics-first modeling

---

## Summary

The Silver layer provides reliable, well-structured data that serves as the foundation for business-level aggregations in the Gold layer.

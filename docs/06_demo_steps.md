# 🎬 Demo Steps — End-to-End Walkthrough

This document explains how to **demonstrate the full pipeline flow** of  
**Olist Analytics: Pseudo Realtime Dashboard using Airflow and Azure**.

The goal is to clearly show how new data moves from ingestion to dashboard updates.

---

## 🎯 Demo Objective

This demo is designed to prove that:

- the pipeline is automated
- data is processed incrementally
- dashboards update after each successful batch
- the system behaves like near real-time analytics

---

## Step 1 — Initial Dashboard State

Start by opening the Power BI dashboard.

What to highlight:
- current KPIs
- last available date or batch
- existing trends

This establishes a **baseline** before new data arrives.

---

## Step 2 — Seed a New Batch

To simulate new data arrival:

1. Copy a subset of raw Olist CSV files into: data/raw/olist/incoming/
2. Create a batch marker file: BATCH<id>.ready

This represents a **new micro-batch event**.

---

## Step 3 — Airflow Detection

Once the marker is present:

- Airflow scheduler detects the new batch
- The DAG evaluates:
- marker existence
- previous checkpoint state

If the batch is new, the pipeline proceeds automatically.

---

## Step 4 — Pipeline Execution

Airflow executes the following phases sequentially:

1. Bronze ingestion
2. Silver transformation
3. Gold aggregation
4. Export to CSV
5. Publish to Azure PostgreSQL
6. Checkpoint update

Each task must succeed before the next one starts.

---

## Step 5 — DAG Completion

After successful execution:

- The DAG ends in a success state
- The batch is recorded as processed
- No duplicate processing occurs on re-runs

This confirms **idempotent behavior**.

---

## Step 6 — Dashboard Update

Finally:

1. Refresh the Power BI dashboard
2. Observe:
- new dates
- updated KPIs
- refreshed trends

The dashboard now reflects the newly processed batch.

---

## Recommended Demo Flow

For interviews or presentations:

1. Show dashboard (before)
2. Seed new batch
3. Show Airflow DAG running
4. Show DAG success
5. Refresh dashboard
6. Explain updated metrics

This flow clearly demonstrates the pseudo-realtime concept.

---

## Summary

This demo proves that:

- the pipeline is automated
- batch-driven processing works as intended
- BI updates are consistent and reliable


# ▶️ Runbook — How to Run & Demo the Pipeline

This runbook explains how to **run the Olist Analytics pipeline end-to-end** and how to **demo it effectively**.

It is written for:
- reviewers
- interviewers
- or anyone cloning the project locally

---

## Prerequisites

Before running the pipeline, make sure you have:

- Docker & Docker Compose installed
- Python environment (if running scripts locally)
- Access to Azure PostgreSQL
- Power BI Desktop (for dashboard viewing)

---

## Environment Setup

1. Clone the repository
2. Prepare environment variables:
   - Database host, port, user, password
   - Target schema: `analytics`
3. Ensure Docker containers can access the database

No secrets are hardcoded in the repository.

---

## Step 1 — Seed a New Batch

To simulate new data arrival:

1. Select a subset of raw Olist CSV files
2. Copy them into: data/raw/olist/incoming/
3. Create a marker file: BATCH<id>.ready


This marker represents a **new batch event**.

---

## Step 2 — Airflow Detection & Orchestration

- Airflow runs on a schedule
- Each DAG run:
- checks for new batch markers
- validates checkpoint state
- decides whether to process or skip

If no new batch is found:
- the DAG exits successfully (no-op)

---

## Step 3 — Data Processing Flow

When a new batch is detected, Airflow executes:

1. **Bronze ingestion**
- Raw CSV → structured parquet
2. **Silver transformation**
- Fact and dimension tables
3. **Gold aggregation**
- KPIs and BI-ready datasets
4. **Export**
- Gold tables → CSV files
5. **Publish**
- CSV → Azure PostgreSQL

Each step must succeed before moving forward.

---

## Step 4 — Checkpoint & Completion

After successful processing:

- The batch ID is written to a checkpoint
- The same batch will not be reprocessed
- Notifications are triggered

This ensures idempotent and safe execution.

---

## Step 5 — Power BI Refresh

Once data is published:

- Power BI connects to `analytics.vw_pbi_unified`
- Dashboards can be refreshed
- New data becomes immediately visible

No manual data modeling is required in the database.

---

## Demo Flow (Recommended)

For demos or interviews:

1. Show empty or old dashboard state
2. Seed a new batch
3. Trigger or wait for Airflow run
4. Show DAG success
5. Refresh Power BI
6. Highlight updated KPIs

This clearly demonstrates the pseudo-realtime behavior.

---

## Common Notes

- The pipeline is safe to run repeatedly
- No new batch ≠ no failure
- Failures stop downstream tasks
- Re-runs do not duplicate data

---

## Summary

This runbook ensures that anyone can:

- run the pipeline confidently
- understand the execution flow
- demo the system in a clear and structured way

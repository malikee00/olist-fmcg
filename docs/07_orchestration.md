# 🔄 Orchestration & Workflow (Airflow)

This document explains how **Apache Airflow** orchestrates the end-to-end data pipeline in  
**_Olist Analytics: Pseudo Realtime Dashboard using Airflow and Azure_**.

The orchestration layer is designed to feel **production-like**, while remaining simple, deterministic, and easy to reason about.

---

## 🎯 Orchestration Objectives

The Airflow workflow is built to achieve the following:

- Fully automated execution (no manual trigger required)
- Batch-driven processing (pseudo-realtime)
- Clear dependency management between pipeline stages
- Safe no-op execution when no new data arrives
- Explicit success and failure notification
- Checkpoint-based idempotency

---

## 🧩 DAG Overview

At a high level, the DAG performs these steps:

1. Detect whether a **new batch** is available
2. Skip execution safely if no batch is found
3. Process data through **Bronze → Silver → Gold**
4. Export and publish results to **Azure PostgreSQL**
5. Update batch checkpoints
6. Send success or failure notifications

---

## 📷 Airflow DAG Visualization

The following diagram shows the complete Airflow DAG used in this project.

![Airflow DAG Flow](./images/airflow_dag.png)

---

## 🧠 DAG Entry & Control Flow

### ▶️ `start`

- Entry point of the DAG
- Acts as a clean visual anchor
- Does not contain any processing logic

---

### 🔀 `branch_marker_checkpoint`

Purpose:
- Detect the presence of a new batch
- Check whether the batch has already been processed

What it evaluates:
- Existence of `_BATCH_<id>.ready` marker
- Current checkpoint state from previous runs

Possible outcomes:
- **New batch detected** → pipeline continues
- **No new batch** → pipeline exits safely

This task is the key to making the DAG **batch-driven and idempotent**.

---

### ⏭️ `noop`

- Executed when **no new batch** is found
- Marks the DAG run as **successful**
- Prevents unnecessary computation
- Ensures the scheduler can run frequently without risk

This design makes the pipeline **safe to automate**.

---

## 🥉 Bronze Processing Phase

### ⚙️ `phase2_bronze`

Responsibilities:
- Ingest raw CSV files from the incoming batch
- Standardize schema and data types
- Append data to the Bronze storage
- Update ingestion checkpoints

Key characteristics:
- Append-only processing
- Batch-aware execution
- No business logic applied

This phase establishes a clean and reliable foundation for downstream processing.

---

## 🥈 Silver Processing Phase

### ⚙️ `phase3_silver`

Responsibilities:
- Transform Bronze data into analytic-ready datasets
- Build fact and dimension tables
- Enforce consistent grain and relationships

Outputs include:
- `fact_orders`
- `fact_order_items`
- `dim_customers`
- `dim_products`

This phase focuses on **data correctness and usability**, not BI presentation.

---

## 🥇 Gold Processing Phase

### ⚙️ `phase4_gold`

Responsibilities:
- Aggregate Silver data into business KPIs
- Produce BI-friendly datasets

Outputs include:
- Daily and monthly KPIs
- State-level metrics
- Payment mix and top products
- BI-only derivative table (`pbi_fact_daily`)

This phase represents the **business logic layer** of the pipeline.

---

## 📦 Export & Publish

### 📤 `export_gold_csv`

- Exports Gold-layer outputs into CSV files
- Stores results in `data/gold_exports/`
- Acts as a stable interface between Spark and PostgreSQL

This step decouples **processing** from **serving**.

---

### ☁️ `publish_postgres`

- Loads exported CSV files into **Azure PostgreSQL**
- Uses a simple and deterministic strategy:
  - `TRUNCATE` existing tables
  - `COPY` new data
- Targets schema: `analytics`

Azure PostgreSQL becomes the **final serving layer**, not a transformation engine.

---

## 🧾 Checkpointing & Completion

### ✅ `checkpoint_processed_batch`

- Records the successful processing of the current batch
- Prevents duplicate reprocessing
- Enables safe re-runs and retries

This task is critical for **idempotency and reliability**.

---

## 🔔 Notifications

### 🎉 `notify_success`

- Triggered after all tasks complete successfully
- Confirms that:
  - the batch was processed
  - data was published
  - dashboards are ready to be refreshed

---

### 🚨 `notify_failure`

- Triggered if **any critical task fails**
- Centralizes failure handling
- Makes issues visible immediately

---

## 🔁 Automation & Scheduling

Key automation principles:
- The DAG can be scheduled frequently
- Each run checks for new batches
- Runs without new data exit cleanly
- Only unprocessed batches are executed

This design mimics **real-world incremental pipelines** without requiring streaming infrastructure.

---

## 🧠 Why This Design Works Well

- Simple control flow, easy to debug
- Clear separation of concerns
- Safe to automate at scale
- Easy to explain during interviews
- Production-inspired, portfolio-friendly

---

## ✅ Summary

The Airflow DAG in this project demonstrates:

- Batch-driven pseudo-realtime orchestration
- Strong idempotency via checkpoints
- Clean handling of no-data scenarios
- Reliable promotion from raw data to BI-ready outputs

It serves as the **control backbone** of the entire analytics system.

# Bronze Layer — Batch Semantics & Ingestion Rules

## 2.2 Batch Semantics

### Purpose

Pseudo-realtime ingestion requires a **clear and consistent definition**
of what constitutes a “batch”.

Without an explicit batch definition:
- ingestion becomes ambiguous
- checkpointing becomes unreliable
- retries can cause duplication
- auditability is lost

This section defines **what one batch means** in this project
and how batch identity is used throughout the Bronze layer.

---

## What Is One Batch?

A **batch** is defined as:

> A complete set of required CSV files that appear together
> in the `data/raw/olist/incoming/` directory.

For this project, one batch must contain **all core tables**
used by the pipeline.

### Required files per batch (minimum)

- orders dataset
- order items dataset
- products dataset
- customers dataset
- payments dataset

If one or more required files are missing,
the batch is considered **invalid** and must not be ingested.

---

## Batch Atomicity

A batch is treated as **atomic**, meaning:

- either all files in the batch are processed successfully
- or none of them are considered processed

Partial ingestion is **not allowed**.

This guarantees:
- consistency across tables
- reliable joins in later stages
- safe retries on failure

---

## Batch Identification (Batch ID)

Each batch is assigned a **Batch ID**.

The Batch ID uniquely identifies:
- one ingestion event
- one set of raw files
- one Bronze append operation

### Supported Batch ID Format

This project uses a **timestamp-based Batch ID**:

batch_YYYYMMDD_HHMM

Example: batch_20250115_0930


Rationale:
- easy to read
- sortable
- collision-safe for local simulation

---

## Where Batch ID Is Used

The Batch ID is a core concept in the Bronze layer and is used for:

1. **Partitioning Bronze data**
   - Each Bronze write is associated with one Batch ID

2. **Checkpointing**
   - Tracking which batch has already been processed

3. **Audit & Metadata Logging**
   - Recording ingestion time
   - Recording row counts per table
   - Tracing lineage from raw → bronze

---

## Batch Lifecycle (High-Level)

1. A full set of CSV files is copied into `incoming/`
2. The pipeline detects the presence of a valid batch
3. A Batch ID is generated
4. Ingestion reads all files in the batch
5. Bronze data is written using append semantics
6. Batch metadata is recorded
7. Raw files are moved to `archive/`

If any step fails:
- the batch is not marked as processed
- raw files remain in `incoming/` for retry

---

## Out of Scope for This Section

This section does **not** define:
- ingestion implementation details
- validation rules
- checkpoint file format
- Spark or Airflow code

Those are defined in later steps of Phase 2.




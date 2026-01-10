# 🥉 Bronze Layer — Raw but Structured Data

The Bronze layer represents the **first landing zone** for incoming data in  
**Olist Analytics: Pseudo Realtime Dashboard using Airflow and Azure**.

Its primary goal is to ingest raw data safely and consistently, without applying business logic.

---

## Purpose of the Bronze Layer

The Bronze layer is designed to:

- Capture raw data exactly as it arrives
- Standardize schemas and data types
- Preserve data lineage and traceability
- Enable safe reprocessing and recovery

This layer acts as the **system of record** for ingested data.

---

## Input Source

Data is ingested from:

- `data/raw/olist/incoming/`
- Each batch is identified using a `_BATCH_<id>.ready` marker

Only new, unprocessed batches are ingested.

---

## Processing Characteristics

Key characteristics:

- Append-only ingestion
- Batch-aware execution
- No joins or aggregations
- No business rules applied

The focus is on **data integrity**, not analytics.

---

## Output Storage

Bronze outputs are stored as:

- Parquet files
- Partitioned by batch or ingestion timestamp
- Stored under: data/bronze/olist_parquet/


Metadata and logs are stored separately for audit purposes.

---

## Checkpointing

After successful ingestion:

- A checkpoint state is updated
- The same batch will not be reprocessed
- Duplicate ingestion is prevented

This ensures **idempotent behavior**.

---

## Design Principles

- Preserve raw data
- Avoid premature transformation
- Enable reproducibility
- Keep the layer simple and reliable

---

## Summary

The Bronze layer provides a clean and trustworthy foundation by ensuring that all incoming data is captured consistently and safely before further processing.
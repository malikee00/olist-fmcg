# Bronze Layer — Batch Semantics & Bronze Contract

---

## 2.2 Batch Semantics

### Purpose

Pseudo-realtime ingestion requires a clear and consistent definition of what
constitutes a **batch**.

Without a strict batch definition, incremental ingestion, retry behavior,
and checkpointing cannot be implemented reliably.

This section defines what a batch means in this project and how it is treated
throughout the Bronze layer.

---

### Definition of One Batch

A **batch** is defined as:

> One complete set of required CSV files that appear together  
> in the `data/raw/olist/incoming/` directory.

A valid batch **must contain all required core tables** used by the pipeline:

- orders
- order_items
- products
- customers
- payments

If one or more required files are missing, the batch is considered **invalid**
and must **not** be ingested.

---

### Batch Atomicity

A batch is treated as **atomic**:

- Either **all files** in the batch are processed successfully
- Or **none of them** are considered processed

Partial ingestion is **not allowed**.

This guarantees:

- consistency across tables
- reliable downstream joins
- safe retry behavior

---

### Batch ID

Each batch is assigned a unique **Batch ID**.

The project uses a **timestamp-based format**:

```
batch_YYYYMMDD_HHMM
```

Example:

```
batch_20250115_0930
```

The Batch ID is used for:

- partitioning Bronze data
- checkpointing processed batches
- audit and lineage tracking

---

### Batch Lifecycle (High-Level)

1. A full set of CSV files is copied into `incoming/`
2. The presence of a valid batch is detected
3. A Batch ID is generated
4. All files in the batch are ingested
5. Bronze data is written using append semantics
6. Metadata is recorded
7. Raw files are moved to `archive/`

If ingestion fails at any point:

- the batch is **not** marked as processed
- raw files remain in `incoming/`
- the batch can be retried safely

---

## 2.3 Bronze Contract (Pseudo-Realtime)

### Purpose

The Bronze layer is the **first persisted layer** in the pipeline.

Its role is to store raw data in a structured, queryable format
while preserving the original information as much as possible.

Bronze data must be:

- raw but readable
- append-only
- ready for incremental processing

---

### Definition of Bronze Data

Bronze data is defined as:

> Raw data ingested from CSV files, minimally standardized,  
> and stored in a columnar format suitable for downstream processing.

The Bronze layer **does not perform**:

- business logic
- aggregations
- cross-table joins
- enrichment or derivation

---

### Bronze Ingestion Rules

#### Rule 1 — Incremental Ingestion Only

- Ingestion reads **only new files** from `incoming/`
- Full refresh is **not allowed**
- One ingestion run processes **exactly one batch**

---

#### Rule 2 — One Source File Maps to One Bronze Table

- Each source CSV file maps to **one Bronze table**
- Tables are written **independently**
- **No joins** are allowed at Bronze level

---

#### Rule 3 — Storage Format

- All Bronze outputs are stored as **Parquet**
- CSV is used **only** at the raw ingestion boundary

---

#### Rule 4 — Append and Partition Strategy

Bronze tables must be **append-only** and **partition-ready**.

The recommended strategy is to partition by `batch_id`.

Logical example:

```
data/bronze/olist_parquet/
└─ olist_orders_dataset/
   └─ batch_id=batch_20250115_0930/
      └─ part-0000.parquet
```

This enables:

- clean incremental ingestion
- batch-level traceability
- safe rollback and retry

---

#### Rule 5 — Column Standardization

Minimal standardization is applied:

- column names are lowercase and snake_case
- timestamp columns are cast to timestamp types
- numeric fields are cast where applicable

No business transformation is allowed.

---

#### Rule 6 — No Empty Outputs

For each batch:

- Bronze tables must contain **at least one row**
- Empty outputs are **not allowed**

If a required table produces zero rows:

- the batch is considered **invalid**
- ingestion must **fail**
- raw files remain in `incoming/`

---

### Bronze Output Structure

All Bronze data is written under:

```
data/bronze/olist_parquet/
```

Expected logical structure:

```
data/bronze/olist_parquet/
├─ olist_orders_dataset/
├─ olist_order_items_dataset/
├─ olist_products_dataset/
├─ olist_customers_dataset/
├─ olist_order_payments_dataset/
└─ _meta/
```

Each table directory contains append-only Parquet files
organized by `batch_id` partitions.

---

### Metadata and Audit Logs (Optional)

Metadata files may be generated to support auditing and demo purposes.

Location:

```
data/bronze/olist_parquet/_meta/
```

File naming convention:

```
batch_<batch_id>.json
```

Suggested metadata contents:

- batch_id
- ingestion timestamp
- list of processed raw files
- row count per Bronze table
- ingestion status

---

### Failure and Retry Behavior

If Bronze ingestion fails:

- no Bronze data is considered valid
- checkpoint state must **not** be updated
- raw files remain in `incoming/`
- the batch can be retried safely

Partial success is **not allowed**.

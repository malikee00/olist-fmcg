# PHASE 3 — Transform → Silver (Incremental & Idempotent)

## Step 3.1 — Batch Context (Dependency on Phase 2)
**Inputs:**
* `data/checkpoints/bronze_ingest_state.json` → To retrieve the `last_batch_id`.
* Bronze Parquet tables partitioned by `batch_id` in `data/bronze/olist_parquet/<table>/`.

**Gate Rules:**
* Phase 3 reads the `last_batch_id` from the Bronze state.
* If the checkpoint is empty or no new batch is detected: **NO-OP** (No Operation) and exit success.
* **Silver Guard Logic**: 
    * Managed via `data/checkpoints/silver_transform_state.json`.
    * If `last_processed_batch_id == last_batch_id`: **NO-OP** to prevent rerun duplicate appends.

---

## Step 3.2 — Silver Contract (Canonical Analytic-Ready)

### 🛠 Transformation Rules
* **Naming Convention**: All column names must be `lowercase_underscore` (snake_case).
* **Data Types**:
    * Timestamps: Convert string timestamps to Spark `TimestampType`.
    * Numerics: Convert currency fields (price/freight) to `DoubleType`.
    * Nulls: Fill missing categorical values with `"unknown"`.

### 📐 Table Grains
| Table | Grain | Key |
| :--- | :--- | :--- |
| `fact_orders` | 1 row per order | `order_id` |
| `fact_order_items` | 1 row per order item | `order_id`, `order_item_id` |
| `dim_customers` | 1 row per unique customer | `customer_id` |
| `dim_products` | 1 row per unique product | `product_id` |

### 🔄 Incremental Strategy
* **Facts (Fact Tables)**: 
    * Strategy: **Partition Overwrite** by `batch_id`.
    * Benefit: Idempotent; rerunning the same batch replaces data instead of duplicating it.
* **Dims (Dimension Tables)**: 
    * Strategy: **Upsert** (Update + Insert).
    * Logic: `union(existing_dims, current_batch) → dropDuplicates(key) → overwrite target`.

---

## Step 3.3 — Output & Quality Control

###  Storage Layout
* `data/silver/olist_parquet/dim_customers/`
* `data/silver/olist_parquet/dim_products/`
* `data/silver/olist_parquet/fact_orders/`
* `data/silver/olist_parquet/fact_order_items/`

###  Data Quality (DQ) Gates
1. **Volume**: Fact tables for the processed batch must have `row_count > 0`.
2. **Integrity**: Primary Keys (PKs) must not be `NULL`.
3. **Uniqueness**: PKs must be unique according to defined grains within the batch.
4. **Consistency**: Dimension tables must not be empty after merging.
5. **Coverage**: Categorical column data coverage must be `>= 70%`.
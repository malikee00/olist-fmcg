## Step 2.5 — Seed incoming (simulate file arrival)

### Purpose
Because the Kaggle dataset is static, we simulate “new data arrival” by copying
a full set of required CSV files into `data/raw/olist/incoming/`.

This represents one atomic micro-batch.

### Prerequisites
Ensure the full extracted dataset CSVs exist in:

- `data/raw/olist/reference/`

Required files:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_payments_dataset.csv`

### Run (PowerShell)
From the project root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\seed_incoming.ps1
If incoming/ is not empty:

powershell
Salin kode
powershell -ExecutionPolicy Bypass -File scripts\seed_incoming.ps1 -CleanIncoming
Expected Result
After running, incoming/ contains:

the 5 required CSV files

a batch marker file:

_BATCH_<batch_id>.ready

This marker will be used by the Bronze ingestion job to detect a complete batch.
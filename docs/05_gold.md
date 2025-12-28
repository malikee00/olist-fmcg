# Phase 4 — Gold Layer (KPI-Ready)

## 🎯 Purpose
The Gold Layer serves as the final consumption layer for Business Intelligence (BI) and Dashboards. It provides:
* **Aggregated**: Pre-computed metrics for faster reporting.
* **Clean & Consistent**: Standardized business logic applied across all metrics.
* **Incremental Updates**: Pseudo-realtime refreshes based on the latest processed batches.

---

## 📂 Output Structure
Target location: `data/gold/olist_parquet/`
* `kpi_daily/`: Daily performance metrics (partitioned by `order_date`).
* `kpi_monthly/`: Monthly performance metrics (partitioned by `order_month`).
* `top_products/`: Top-performing categories (partitioned by `order_month`).
* `payment_mix/`: Distribution of payment methods (partitioned by `order_month`).
* `kpi_by_state/`: Regional performance (partitioned by `order_month`).
* `_meta/`: Execution logs for each batch (`batch_<id>.json`).

---

## 🔄 Incremental Window Strategy (IDEMPOTENT)
To ensure accuracy and prevent duplicates, we use the **"Affected Partitions"** strategy:
1. **Identify**: Read the latest batch from Silver `fact_orders`.
2. **Extract**: Determine the distinct list of `order_date` and `order_month` present in that batch.
3. **Recompute & Overwrite**: 
    * Recalculate only the specific partitions (days/months) that contain new or updated data.
    * Use **Partition Overwrite** to replace the affected folders.

**Benefits**:
* **Anti-Duplicate**: No risk of double-counting rows.
* **Safe Reruns**: Supports `-Force` commands without corrupting history.
* **BI Stability**: Dashboards always read a consistent state of truth.

---

## 🛠 Business Filters
* **Standard Policy**: `delivered_only = true` 
    * Only orders with `order_status = 'delivered'` are included in revenue and growth KPIs.

---

## 📊 Grains & Metrics Definitions

### 1. Daily KPIs (`kpi_daily`)
* **Grain**: `order_date`
* **Revenue**: `SUM(price)`
* **Orders**: `COUNT DISTINCT(order_id)`
* **AOV**: Average Order Value (`revenue / orders`)

### 2. Monthly KPIs (`kpi_monthly`)
* **Grain**: `order_month`
* **Metrics**: Total Revenue, Total Orders, and Monthly AOV.
* *Note: Month-over-Month (MoM) growth is calculated at the SQL View or BI tool level.*

### 3. Top Categories (`top_products`)
* **Grain**: `order_month` x `product_category_name`
* **Metrics**: Total Revenue, Order Volume, Average Price, and Average Freight.

### 4. Payment Mix (`payment_mix`)
* **Grain**: `order_month` x `payment_type`
* **Metrics**: `total_payment_value` and percentage share of total monthly revenue.

### 5. Regional KPIs (`kpi_by_state`)
* **Grain**: `order_month` x `customer_state`
* **Metrics**: Revenue and Order Count per state.
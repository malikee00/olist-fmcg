# Phase 5 — SQL Views (BI Contract)

## 🎯 Principles
* **Stable API**: Views act as the permanent interface for Business Intelligence (BI) tools like Looker Studio, Power BI, or Metabase.
* **Incremental Flexibility**: While the Gold layer updates incrementally, the logical structure of the Views remains unchanged, ensuring dashboards do not break.
* **Decoupled Logic**: Complex analytical calculations (like rankings and growth rates) are handled here to keep the Gold layer focused on storage.

---

## 🗺️ Mapping Layout
The following views map directly to the Gold Parquet tables:

| BI View Name | Source Gold Table | Added Logic/Calculations |
| :--- | :--- | :--- |
| `analytics.vw_kpi_daily` | `gold.kpi_daily` | None (Direct Map) |
| `analytics.vw_kpi_monthly` | `gold.kpi_monthly` | Month-over-Month (MoM) Growth |
| `analytics.vw_top_categories` | `gold.top_products` | Category Ranking (Rank) |
| `analytics.vw_payment_mix` | `gold.payment_mix` | None (Share pre-calculated) |
| `analytics.vw_kpi_by_state` | `gold.kpi_by_state` | None (Direct Map) |

---

## 📝 Technical Notes
* **Growth Analytics**: MoM (Month-over-Month) growth is computed dynamically within the view logic rather than stored in the Gold layer.
* **Ranking Logic**: Top-N ranking (e.g., finding the top 5 categories) is performed at the view level for maximum flexibility.
* **Pre-calculated Shares**: Percentage shares for `payment_mix` are stored directly in Gold to simplify the visualization process in BI tools.
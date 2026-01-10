# 📖 Data Dictionary

This document provides a **high-level data dictionary** for key datasets used in  
**Olist Analytics: Pseudo Realtime Dashboard using Airflow and Azure**.

It focuses on fields commonly consumed by the BI layer.

---

## BI Contract View

Primary BI source: analytics.vw_pbi_unified


---

## Common Fields

| Column Name | Description |
|-----------|-------------|
| date | Transaction or aggregation date |
| month_date | Month-level representation |
| customer_state | Customer location (state) |
| payment_type | Payment method |
| product_category | Product category |
| revenue | Total revenue |
| orders | Number of orders |
| total_payment_value | Total payment amount |
| avg_price | Average item price |
| avg_freight | Average freight cost |
| mom_revenue_growth | Month-over-month revenue growth |
| mom_orders_growth | Month-over-month order growth |
| payment_share | Contribution percentage by payment |
| batch_id | Processed batch identifier |
| updated_at | Last update timestamp |

---

## Grain Definition

The BI contract is designed at:

- **Daily grain**
- One row per:
  - date
  - state
  - payment type
  - category

This grain supports both aggregation and drill-down.

---

## Notes on Calculations

- Core aggregations are handled in the Gold layer
- Advanced ratios and growth metrics may be computed in Power BI (DAX)
- The database focuses on consistency and stability

---

## Change Policy

- Columns are added, not removed
- Existing columns maintain semantic meaning
- Breaking changes are avoided at the BI contract level

---

## Summary

This data dictionary ensures shared understanding between:

- data engineering
- analytics
- BI consumers

and acts as a reference for dashboard development.

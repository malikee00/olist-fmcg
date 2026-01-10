# 🧾 SQL Views & BI Contract

This document explains the role of SQL views in  
**Olist Analytics: Pseudo Realtime Dashboard using Airflow and Azure**.

The primary focus is the **BI contract** used by Power BI.

---

## Purpose of SQL Views

SQL views are used to:

- abstract table complexity
- standardize column definitions
- provide a stable interface for BI tools
- prevent breaking changes in dashboards

---

## BI Contract Definition

The BI contract is defined as: analytics.vw_pbi_unified


Power BI connects **only to this view**.

---

## Why a Single BI Contract?

Using a single unified view ensures:

- consistent slicer behavior
- predictable joins and grain
- simplified Power BI modeling
- safer downstream changes

This approach treats the database as an **API layer**.

---

## Underlying Data Sources

The unified view is built on top of:

- Gold KPI tables
- BI-specific derivative tables (e.g. `pbi_fact_daily`)
- Supporting dimension attributes

The complexity is hidden from the BI layer.

---

## Expected Grain

The view is designed at a **daily grain**, with attributes such as:

- date
- customer state
- payment type
- product category

This grain supports both aggregation and drill-down.

---

## Change Management

If business logic changes:

- underlying tables can be updated
- the unified view absorbs changes
- Power BI remains unaffected

This protects dashboards from schema instability.

---

## Design Principles

- Backward compatibility
- Clear ownership of logic
- BI-friendly schema
- Minimal coupling to storage tables

---

## Summary

The SQL view layer plays a critical role by:

- enforcing a single source of truth
- simplifying BI consumption
- enabling safe evolution of the data model
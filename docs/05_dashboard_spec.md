# 📊 Dashboard Specification — Power BI

This document describes how data from the analytics layer is mapped into  
**Power BI visuals** for the Olist Analytics dashboard.

---

## Dashboard Overview

The dashboard is divided into **three pages**, each serving a different purpose:

1. Executive Overview
2. Composition & Business Insight
3. Detail & Evidence

All visuals consume data from a **single BI contract**.

---

## Data Source

Power BI connects to: analytics.vw_pbi_unified


This view acts as the **only source of truth** for the dashboard.

---

## Common Filters & Slicers

Available slicers across the dashboard:

- Date / Month
- Customer State
- Payment Type
- Product Category

These slicers are designed to work consistently across all pages.

---

## Page 1 — Executive Overview

Purpose:
- High-level business performance

Typical visuals:
- KPI cards (Revenue, Orders, AOV)
- Line chart (Revenue trend)
- Month-over-month growth indicators

Audience:
- Executives
- Decision makers

---

## Page 2 — Composition & Business Insight

Purpose:
- Understand performance breakdowns

Typical visuals:
- Revenue by state
- Payment method contribution
- Category performance
- Share and percentage visuals

Audience:
- Analysts
- Business stakeholders

---

## Page 3 — Detail & Evidence

Purpose:
- Drill-down and validation

Typical visuals:
- Detailed tables
- Transaction-level summaries
- Supporting metrics for audit

Audience:
- Analysts
- Reviewers

---

## Measures & Calculations

Key calculations are implemented using **DAX**, including:

- Month-over-Month growth
- Payment share percentage
- Contribution ratio
- Average Order Value (AOV)

These are intentionally handled in Power BI, not in the database.

---

## Design Principles

- Single source of truth
- Minimal data duplication
- Consistent slicer behavior
- Clear separation between data and presentation

---

## Summary

The dashboard specification ensures that:

- visuals remain consistent
- filters behave predictably
- business insights are easy to interpret



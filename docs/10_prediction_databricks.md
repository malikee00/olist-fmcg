# 📈 Daily Revenue Prediction — Analytics-Driven ML

## Overview
This module implements a **next-day revenue prediction** using business-ready analytics data.  
The model is trained in **Databricks** and its output is consumed directly by **Power BI** for monitoring, evaluation, and alerting.

The focus is **practical analytics ML**, not experimental modeling.

---

## 🎯 Business Objective
Forecast **tomorrow’s revenue** based on recent sales patterns to:
- Provide early signals of revenue changes
- Support operational and commercial planning
- Enable monitoring and alerting in BI dashboards

---

## 🗂 Data Source

**Dataset**
analytics.vw_kpi_daily


**Layer**
- Gold (analytics-ready)

**Characteristics**
- Clean & trusted
- Same source used by Power BI
- Optimized for fast, low-cost ML workloads

**Columns**
- `date`
- `orders`
- `revenue`
- `aov`
- `batch_id`
- `updated_at`

---

## 🔮 Prediction Target

**Label**
revenue_next_day


**Definition**
Revenue on day *t + 1*, predicted using information available on day *t*.

---

## 🧩 Feature Engineering

The dataset is transformed into a supervised learning format.

**Features (day t)**
- `orders`
- `revenue`
- `aov`
- `revenue_lag_1`
- `revenue_lag_7`
- `revenue_ma_7`
- `orders_ma_7`
- `day_of_week`

**Label**
- `revenue_next_day`

This feature set balances **simplicity, interpretability, and predictive signal**.

---

## 🤖 Model Training

**Algorithm**
- `GBTRegressor` (Spark ML)

**Rationale**
- Non-linear relationships
- Fast training time
- Minimal tuning required
- Native integration with Spark & Databricks

**Artifacts**
- Model stored in DBFS / MLflow
- Versioned for traceability

---

## 📊 Model Evaluation

**Train / Test Split**
- 80% earliest data → training
- 20% most recent data → testing  
(Time-aware split to avoid data leakage)

**Metrics**
- RMSE
- MAPE

**Baseline Comparison**
- Naïve forecast:  
  `revenue(t + 1) = revenue(t)`

**Evaluation Principle**
The model must outperform the baseline to justify production usage.

---

## 🚀 Batch Scoring

Predictions are generated using the **latest available day** to forecast the next day.

**Output Schema**
| Column | Description |
|------|------------|
| `date` | Last observed date |
| `pred_date` | Prediction date (date + 1) |
| `forecast_revenue_next_day` | Predicted revenue |
| `actual_revenue_next_day` | Actual revenue |
| `abs_error` | Absolute error |
| `model_version` | Model identifier |
| `scored_at` | Scoring timestamp |
| `batch_id` | Analytics batch reference |

---

## 🗄 Data Contract for BI

**Target Table**
analytics.ml_pred_kpi_daily


This table serves as the **contract between ML and Power BI**, enabling:
- Actual vs Forecast comparison
- Error tracking
- Trend monitoring
- Business alerting

---

## 📉 Power BI Consumption

**Key Metrics**
- Actual Revenue
- Forecast Revenue
- Absolute Error
- MAPE %
- Accuracy (1 − MAPE)

**Primary Use Cases**
- Monitor forecast performance over time
- Detect sudden deviations or drift
- Support decision-making with forward-looking insights

---

## ⚠️ Alerting Logic (Optional)

A simple business rule can be applied:
- Flag high error when MAPE exceeds a defined threshold (e.g. 15%)

This enables proactive investigation and action.

---

## 🧠 Design Principles

- Analytics-first, ML-second
- Cost-efficient and scalable
- Transparent and explainable
- Fully integrated with BI workflows

---

## Summary
This prediction pipeline demonstrates how **Gold analytics data** can be leveraged to build **lightweight, production-ready ML** that delivers immediate value through business dashboards.

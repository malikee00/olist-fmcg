# 📌 One Pager — Olist Analytics

## Project Title
**Olist Analytics: Pseudo Realtime Dashboard using Airflow and Azure**

---

## Problem Statement

In real-world analytics systems, business stakeholders expect dashboards to update frequently as new data arrives.  
However, building true real-time streaming pipelines is often complex, costly, and unnecessary for many use cases.

This project addresses the question:

> *How can we design a production-like analytics system that behaves like real-time, while remaining simple, reliable, and easy to operate?*

---

## Solution Overview

**Olist Analytics** implements a **pseudo-realtime analytics architecture** using batch-based ingestion combined with automated orchestration.

Instead of streaming, data arrives in **small incremental batches**, which are:
- automatically detected,
- processed end-to-end,
- and reflected in dashboards with minimal delay.

This approach mirrors how many real production analytics platforms operate.

---

## Key Design Decisions

- **Batch-driven pseudo-realtime ingestion**  
  New data arrives as micro-batches instead of continuous streams.

- **Airflow as the orchestration backbone**  
  Airflow handles scheduling, branching, retries, and failure handling.

- **Layered data architecture**  
  Clear separation between Bronze, Silver, and Gold layers.

- **Azure PostgreSQL as the serving layer**  
  Acts as a stable, cloud-ready analytics database.

- **Single BI contract**  
  Power BI consumes only one unified SQL view:  
  `analytics.vw_pbi_unified`.

---

## Business Value

This project demonstrates how to:

- Deliver near real-time insights without streaming infrastructure
- Build reliable, repeatable analytics pipelines
- Prevent broken dashboards using a strict BI contract
- Separate data engineering responsibilities from BI modeling
- Create a system that is easy to explain, demo, and maintain

---

## Target Audience

- Data Engineers
- Analytics Engineers
- BI Engineers
- Interviewers evaluating end-to-end analytics skills

---

## Out of Scope

To keep the system focused and realistic:

- Real-time streaming (Kafka, Spark Streaming)
- Complex CDC or event-driven ingestion

These can be added as future enhancements.

---

## Summary

**Olist Analytics** is a portfolio-grade project that showcases:

- production-inspired architecture
- automated orchestration
- clean data modeling
- and reliable BI delivery

All built using simple, well-structured components.

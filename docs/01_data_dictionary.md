# Data Overview & Raw Data Strategy

## 1. Background

In real-world data engineering systems,
data rarely arrives as a single static dataset.
Instead, data arrives incrementally in batches or streams.

Although the Olist dataset is static,
this project is designed to demonstrate
**production-like ingestion behavior**.

---

## 2. Why Pseudo-Realtime Is Used

True real-time streaming is out of scope because:
- the data source is static
- there is no event stream or CDC mechanism
- the project focus is batch-oriented data engineering

Pseudo-realtime ingestion allows the project to demonstrate:
- incremental processing
- idempotent design
- retry-safe ingestion
- orchestration readiness

---

## 3. Data Acquisition Strategy

The dataset is acquired from Kaggle and stored locally.

Raw data is divided into three logical zones:

reference → incoming → archive


Each zone has a clear responsibility:
- **reference**: static source of truth
- **incoming**: simulated data arrival
- **archive**: processed raw data

Only the `incoming` zone is read by ingestion logic.

---

## 4. Simulating Data Arrival

Because the dataset does not change over time,
data arrival is simulated by copying files
from `reference/` into `incoming/`.

Each copy operation represents:
- one new batch
- one ingestion event
- one execution of the pipeline

This enables deterministic testing and demo scenarios.

---

## 5. Design Principles

The raw data design follows these principles:

1. **Separation of concerns**  
   Source data, incoming data, and processed data are isolated.

2. **Immutability**  
   Raw data is never modified after ingestion.

3. **Retry safety**  
   Failed ingestion does not corrupt state.

4. **Reproducibility**  
   The pipeline can be replayed from scratch.

---

## 6. Scope of Step 2.1

### Included
- raw data layout
- pseudo-realtime concept
- ingestion boundaries
- version control rules

### Excluded
- batch identity rules
- ingestion code
- validation logic
- configuration files

These are intentionally deferred.

---

## 7. Next Step

With data acquisition and raw storage defined,
the next step is to formalize how data is grouped into batches.


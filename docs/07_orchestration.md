# Phase 5 — Airflow Orchestration 🚀
Pseudo-Realtime | Incremental | Production-like 

## Why pseudo-realtime? ⏱️
This project simulates near-real-time ingestion via file arrival in `incoming/`,
orchestrated by Airflow micro-batches. True streaming is out of scope because the source dataset is static. 

## DAG schedule 🗓️
- Runs every **10 minutes** (`*/10 * * * *`)
- Important: scheduling is **not** data generation.
  Airflow simply **checks** whether a new batch exists.

## Trigger mechanism (marker file) 🏷️
A new batch is detected via marker files:
- `data/raw/olist/incoming/_BATCH_<id>.ready`

Why markers?
- Prevents partial upload issues 
- Avoids race conditions 
- Makes event simulation clear 

## Anti-duplicate guard 🔐
Airflow compares:
- **marker batch_id** (from `_BATCH_<id>.ready`)
vs
- **checkpoint**: `data/checkpoints/bronze_ingest_state.json` (`last_batch_id`)

Rules:
- No marker => **NO-OP success** 
- Marker exists but `batch_id == last_batch_id` => **NO-OP success** 
- Marker exists and `batch_id != last_batch_id` => run Phase 2 → 3 → 4 

This prevents accidental reruns and duplicate appends.

## Fail-fast gates ⚡
Airflow depends on exit codes from scripts:

| Script | Fails if |
|---|---|
| `run_phase2.ps1` | bronze validation fails |
| `run_phase3.ps1` | silver validation fails |
| `run_phase4.ps1` | gold validation fails |

Exit code contract:
- `0` → continue ✅
- `!= 0` → DAG fails immediately ❌

## DAG flow 🧠
`branch_on_marker_and_checkpoint`
- if no-op → `t_noop` → notify success ✅
- else → `t_phase2_bronze` → `t_phase3_silver` → `t_phase4_gold` → notify success ✅
- any failure → notify failure ❌

## Local development note 🪟
Local development uses **PowerShell** runners.
In Linux-based Airflow deployment, these scripts can be replaced with shell equivalents without changing pipeline logic.

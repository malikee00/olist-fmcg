# Runbook — Local Demo 🧪

## Prerequisites ✅
- Airflow installed in your environment (venv).
- Phase 2–4 scripts already work:
  - `scripts/seed_incoming.ps1`
  - `scripts/run_phase2.ps1`
  - `scripts/run_phase3.ps1`
  - `scripts/run_phase4.ps1`
- Checkpoint exists (or will be created by Phase 2):
  - `data/checkpoints/bronze_ingest_state.json`

## Run Airflow locally 🚀
From project root:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_airflow_local.ps1

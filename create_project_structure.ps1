# create_project_structure.ps1

$root = Get-Location

function New-ProjectDir([string]$path) {
  if (-not (Test-Path $path)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
}

function New-ProjectFile([string]$path, [string]$content = "") {
  $dir = Split-Path $path -Parent
  if ($dir -and -not (Test-Path $dir)) {
    New-ProjectDir $dir
  }
  Set-Content -Path $path -Value $content -Encoding UTF8
}

# --- Top-level files
New-ProjectFile (Join-Path $root "README.md") @"
# Olist FMCG Sales & Demand Pipeline

End-to-end batch + pseudo-realtime (file arrival) data pipeline:
Raw CSV (Olist) -> Bronze (Parquet) -> Silver (Clean/Join) -> Gold (KPI) -> Postgres -> SQL Views -> Looker Studio
"@

New-ProjectFile (Join-Path $root ".gitignore") @"
.venv/
venv/
__pycache__/
*.pyc
.DS_Store

# Data policy: do not commit raw/full data
data/raw/**
!data/raw/olist/README.md

# Airflow artifacts (if you run airflow locally)
airflow/logs/
airflow/airflow.db
"@

New-ProjectFile (Join-Path $root "requirements.txt") @"
pyspark
pyarrow
pandas
pyyaml
"@

# --- configs
New-ProjectDir (Join-Path $root "configs")

New-ProjectFile (Join-Path $root "configs\bronze_tables.yaml") @"
source_dir: data/raw/olist/incoming
bronze_dir: data/bronze/olist_parquet
tables:
  - name: olist_orders_dataset
    file: olist_orders_dataset.csv
  - name: olist_order_items_dataset
    file: olist_order_items_dataset.csv
  - name: olist_products_dataset
    file: olist_products_dataset.csv
  - name: olist_customers_dataset
    file: olist_customers_dataset.csv
  - name: olist_order_payments_dataset
    file: olist_order_payments_dataset.csv
"@

New-ProjectFile (Join-Path $root "configs\silver_models.yaml") @"
# TODO: silver mapping (inputs from bronze -> outputs to silver)
"@

New-ProjectFile (Join-Path $root "configs\gold_kpis.yaml") @"
# TODO: gold KPI definitions (inputs from silver -> outputs to gold)
"@

New-ProjectFile (Join-Path $root "configs\settings.yaml") @"
run_mode: pseudo_realtime

raw_incoming_dir: data/raw/olist/incoming
raw_archive_dir: data/raw/olist/archive
raw_reference_dir: data/raw/olist/reference
checkpoint_dir: data/checkpoints

bronze_dir: data/bronze/olist_parquet
silver_dir: data/silver/olist_parquet
gold_dir: data/gold/olist_parquet
"@

# --- data folders
New-ProjectDir (Join-Path $root "data\raw\olist\incoming")
New-ProjectDir (Join-Path $root "data\raw\olist\archive")
New-ProjectDir (Join-Path $root "data\raw\olist\reference")
New-ProjectDir (Join-Path $root "data\checkpoints")

New-ProjectDir (Join-Path $root "data\bronze\olist_parquet\_meta")
New-ProjectDir (Join-Path $root "data\silver\olist_parquet\_meta")
New-ProjectDir (Join-Path $root "data\gold\olist_parquet\_meta")

New-ProjectFile (Join-Path $root "data\raw\olist\README.md") @"
# Raw data policy (pseudo-realtime)

- Put NEW arriving batches into: data/raw/olist/incoming/
- After processing, files move to: data/raw/olist/archive/
- Optional full extract lives in: data/raw/olist/reference/ (do not commit)

This project simulates file arrival to mimic near-real-time ingestion.
"@

New-ProjectFile (Join-Path $root "data\checkpoints\bronze_ingest_state.json") @"
{ "last_batch_id": null, "processed_files": [] }
"@

New-ProjectFile (Join-Path $root "data\bronze\olist_parquet\README.md") "Bronze layer outputs (append/partition-ready).`n"
New-ProjectFile (Join-Path $root "data\silver\olist_parquet\README.md") "Silver layer outputs (clean + join, incremental-ready).`n"
New-ProjectFile (Join-Path $root "data\gold\olist_parquet\README.md") "Gold layer outputs (KPI aggregates, incremental-ready).`n"

# --- spark_jobs placeholders
New-ProjectDir (Join-Path $root "spark_jobs")
New-ProjectFile (Join-Path $root "spark_jobs\bronze_ingest.py") "# TODO: ingest incoming batch -> bronze`n"
New-ProjectFile (Join-Path $root "spark_jobs\bronze_validate.py") "# TODO: validate bronze outputs`n"
New-ProjectFile (Join-Path $root "spark_jobs\silver_transform.py") "# TODO: transform bronze batch -> silver`n"
New-ProjectFile (Join-Path $root "spark_jobs\silver_validate.py") "# TODO: validate silver outputs`n"
New-ProjectFile (Join-Path $root "spark_jobs\gold_aggregate.py") "# TODO: aggregate silver -> gold (incremental window)`n"
New-ProjectFile (Join-Path $root "spark_jobs\gold_validate.py") "# TODO: validate gold KPI outputs`n"

# --- sql
New-ProjectDir (Join-Path $root "sql\schema")
New-ProjectDir (Join-Path $root "sql\views")
New-ProjectFile (Join-Path $root "sql\README.md") "SQL artifacts: schema init + BI views.`n"
New-ProjectFile (Join-Path $root "sql\schema\00_init.sql") "-- TODO: init schema (if using Postgres)`n"
New-ProjectFile (Join-Path $root "sql\views\vw_kpi_daily.sql") "-- TODO`n"
New-ProjectFile (Join-Path $root "sql\views\vw_kpi_monthly.sql") "-- TODO`n"
New-ProjectFile (Join-Path $root "sql\views\vw_top_products.sql") "-- TODO`n"
New-ProjectFile (Join-Path $root "sql\views\vw_payment_mix.sql") "-- TODO`n"

# --- airflow
New-ProjectDir (Join-Path $root "airflow\dags")
New-ProjectDir (Join-Path $root "airflow\plugins")
New-ProjectFile (Join-Path $root "airflow\README.md") "Airflow DAGs and run instructions.`n"
New-ProjectFile (Join-Path $root "airflow\dags\olist_pipeline_dag.py") "# TODO: DAG (sensor incoming -> phase2 -> phase3 -> phase4)`n"
New-ProjectFile (Join-Path $root "airflow\plugins\README.md") "Optional custom operators/hooks.`n"

# --- docs
New-ProjectDir (Join-Path $root "docs")
New-ProjectFile (Join-Path $root "docs\00_one_pager.md") "# One-pager`n"
New-ProjectFile (Join-Path $root "docs\01_data_dictionary.md") "# Data dictionary`n"
New-ProjectFile (Join-Path $root "docs\02_kpi_definition.md") "# KPI definition`n"
New-ProjectFile (Join-Path $root "docs\03_architecture.md") "# Architecture`n"
New-ProjectFile (Join-Path $root "docs\04_airflow_dag.md") "# Airflow DAG spec`n"
New-ProjectFile (Join-Path $root "docs\05_dashboard_spec.md") "# Dashboard spec (2 pages)`n"
New-ProjectFile (Join-Path $root "docs\06_demo_steps.md") "# Demo steps`n"

# --- dashboards
New-ProjectDir (Join-Path $root "dashboards\looker_studio")
New-ProjectDir (Join-Path $root "dashboards\screenshots")
New-ProjectFile (Join-Path $root "dashboards\looker_studio\page1_exec_overview.md") "# Page 1 - Executive overview spec`n"
New-ProjectFile (Join-Path $root "dashboards\looker_studio\page2_ops_insight.md") "# Page 2 - Ops insight spec`n"
New-ProjectFile (Join-Path $root "dashboards\screenshots\.gitkeep") ""

# --- scripts
New-ProjectDir (Join-Path $root "scripts")
New-ProjectFile (Join-Path $root "scripts\setup_env.ps1") "# TODO: setup venv + deps`n"
New-ProjectFile (Join-Path $root "scripts\seed_incoming.ps1") "# TODO: simulate new batch into data/raw/olist/incoming`n"
New-ProjectFile (Join-Path $root "scripts\run_phase2.ps1") "# TODO: phase2 (ingest + validate) for new batch`n"
New-ProjectFile (Join-Path $root "scripts\run_phase3.ps1") "# TODO: phase3 (silver transform + validate) for batch`n"
New-ProjectFile (Join-Path $root "scripts\run_phase4.ps1") "# TODO: phase4 (gold aggregate + validate) for batch/window`n"
New-ProjectFile (Join-Path $root "scripts\run_all.ps1") "# TODO: chain phase2 -> phase3 -> phase4`n"
New-ProjectFile (Join-Path $root "scripts\run_airflow_local.ps1") "# TODO: start airflow locally (optional)`n"
New-ProjectFile (Join-Path $root "scripts\trigger_dag.ps1") "# TODO: trigger DAG (optional)`n"

# --- tests
New-ProjectDir (Join-Path $root "tests")
New-ProjectFile (Join-Path $root "tests\test_bronze.py") "# TODO tests`n"
New-ProjectFile (Join-Path $root "tests\test_silver.py") "# TODO tests`n"
New-ProjectFile (Join-Path $root "tests\test_gold.py") "# TODO tests`n"

Write-Host "Project structure created in: $root"
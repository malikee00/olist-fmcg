Param(
  [string]$DagId = "olist_pipeline"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[RUN] airflow dags unpause $DagId"
airflow dags unpause $DagId

Write-Host "[RUN] airflow dags trigger $DagId"
airflow dags trigger $DagId

Write-Host "[OK] Triggered DAG: $DagId"

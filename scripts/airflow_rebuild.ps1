$ErrorActionPreference = "Stop"

Write-Host "[INFO] Rebuilding and starting Airflow containers..." -ForegroundColor Cyan

# Try graceful down first
try {
  docker compose -f docker-compose.airflow.yml down | Out-Host
} catch {
  Write-Host "[WARN] docker compose down failed, forcing scheduler removal..." -ForegroundColor Yellow
  docker rm -f olist-airflow-scheduler 2>$null
  docker rm -f olist-airflow-webserver 2>$null
  docker compose -f docker-compose.airflow.yml down | Out-Host
}

# Build (no-cache)
docker compose -f docker-compose.airflow.yml build --no-cache | Out-Host

# Up
docker compose -f docker-compose.airflow.yml up -d | Out-Host

Write-Host "[OK] Airflow containers are up." -ForegroundColor Green
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

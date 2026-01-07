$ErrorActionPreference = "Stop"

$Scheduler = "olist-airflow-scheduler"

Write-Host "[INFO] Checking DB_* env inside scheduler container..." -ForegroundColor Cyan
docker exec -it $Scheduler bash -lc 'echo "DB_HOST=$DB_HOST"; echo "DB_USER=$DB_USER"; echo "DB_NAME=$DB_NAME"; echo "DB_PORT=$DB_PORT"; echo "DB_SCHEMA=$DB_SCHEMA"'

Write-Host ""
Write-Host "[INFO] Running publish via wrapper from inside container..." -ForegroundColor Cyan
docker exec -it $Scheduler bash -lc 'cd /opt/project && bash scripts/load_supabase.sh'

Write-Host ""
Write-Host "[OK] Publish test completed (check logs above)." -ForegroundColor Green

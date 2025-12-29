Param(
  [string]$ProjectRoot = (Resolve-Path ".").Path,
  [string]$AirflowHome = (Join-Path (Resolve-Path ".").Path ".airflow"),
  [int]$WebserverPort = 8080
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "[OK] $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "[ERROR] $msg" -ForegroundColor Red; exit 1 }

function Run($cmd) {
  Info $cmd
  Invoke-Expression $cmd
  if ($LASTEXITCODE -ne 0) { Fail "Command failed: $cmd" }
}

# Guard: airflow CLI must exist
if (-not (Get-Command airflow -ErrorAction SilentlyContinue)) {
  Fail "'airflow' command not found. Ensure apache-airflow is installed in this venv and the venv is activated."
}

Info "ProjectRoot : $ProjectRoot"
Info "AIRFLOW_HOME : $AirflowHome"
Info "Port        : $WebserverPort"

$env:PROJECT_ROOT = $ProjectRoot
$env:AIRFLOW_HOME = $AirflowHome
$env:AIRFLOW__CORE__EXECUTOR = "LocalExecutor"

# Put DB in a short, no-space path (Windows-friendly)
$dbDir = "C:\airflow_local"
New-Item -ItemType Directory -Force -Path $dbDir | Out-Null

$dbPath = Join-Path $dbDir "airflow.db"
$dbPathUnix = $dbPath -replace "\\", "/"
$sqliteConn = "sqlite:////$dbPathUnix"
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = $sqliteConn

Info "SQL_ALCHEMY_CONN : $env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

# Ensure DAG dir exists inside AIRFLOW_HOME
$dagsTarget = Join-Path $AirflowHome "dags"
New-Item -ItemType Directory -Force -Path $dagsTarget | Out-Null

# Copy repo DAGs into AIRFLOW_HOME/dags
$repoDagDir = Join-Path $ProjectRoot "airflow\dags"
if (-not (Test-Path $repoDagDir)) { Fail "Repo DAG dir not found: $repoDagDir" }

Copy-Item -Path (Join-Path $repoDagDir "*.py") -Destination $dagsTarget -Force
Ok "DAGs copied to $dagsTarget"

# Init DB (fail-fast if error)
Run "airflow db init"
Ok "Airflow DB initialized"

# Create admin user (ignore if already exists)
try {
  Run "airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"
  Ok "Created user admin/admin"
} catch {
  Info "User may already exist. Continuing..."
}

Write-Host ""
Write-Host "   Starting Airflow Scheduler + Webserver..." -ForegroundColor Green
Write-Host "   Web UI: http://localhost:$WebserverPort"
Write-Host "   Login : admin / admin"
Write-Host ""

# Start scheduler in a new PowerShell window WITH the same env vars (important!)
$startSchedulerCmd = @"
cd '$ProjectRoot'
`$env:PROJECT_ROOT = '$ProjectRoot'
`$env:AIRFLOW_HOME = '$AirflowHome'
`$env:AIRFLOW__CORE__EXECUTOR = 'LocalExecutor'
`$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = '$sqliteConn'
airflow scheduler
"@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $startSchedulerCmd

# Start webserver in current window
Run "airflow webserver --port $WebserverPort"

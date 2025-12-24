<#
.SYNOPSIS
  Phase 2 workflow runner:
    Seed incoming -> Bronze ingest (spark-submit) -> Archive processed batch

.DESCRIPTION
  - Reads paths from configs/settings.yaml (incoming, archive)
  - Reads required source files from configs/bronze_tables.yaml
  - Detects latest batch via marker: _BATCH_<batch_id>.ready
  - Runs Spark bronze ingest job
  - On success: moves CSV + marker from incoming/ to archive/<batch_id>/
  - On failure: keeps files in incoming/ (safe retry)

.EXAMPLES
  # Normal run: assumes you already seeded
  powershell -ExecutionPolicy Bypass -File scripts\run_phase2.ps1

  # Run with seed first
  powershell -ExecutionPolicy Bypass -File scripts\run_phase2.ps1 -Seed

  # Force clean incoming before seeding (only works with -Seed)
  powershell -ExecutionPolicy Bypass -File scripts\run_phase2.ps1 -Seed -CleanIncoming

  # Set PYSPARK_PYTHON for this session automatically (recommended)
  powershell -ExecutionPolicy Bypass -File scripts\run_phase2.ps1 -UseVenvPython
#>

param(
  [string]$SettingsPath = "configs/settings.yaml",
  [string]$BronzeTablesPath = "configs/bronze_tables.yaml",
  [switch]$Seed,
  [switch]$CleanIncoming,
  [switch]$UseVenvPython
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Fail($msg) {
  Write-Host "[ERROR] $msg" -ForegroundColor Red
  exit 1
}
function Info($msg) {
  Write-Host "[INFO] $msg" -ForegroundColor Cyan
}
function Ok($msg) {
  Write-Host "[OK] $msg" -ForegroundColor Green
}

function Get-ProjectRoot {
  return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Run-PythonInline($projectRoot, $code) {
  $tmpPy = Join-Path $env:TEMP ("py_" + [Guid]::NewGuid().ToString() + ".py")
  try {
    $code | Set-Content -Path $tmpPy -Encoding utf8
    $out = python $tmpPy
    return $out
  } finally {
    if (Test-Path $tmpPy) { Remove-Item $tmpPy -Force }
  }
}

function Read-PathsFromSettings($projectRoot, $settingsRel) {
  $code = @"
from pathlib import Path
import yaml, json

settings_path = Path(r"$projectRoot") / r"$settingsRel"
with open(settings_path, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

paths = cfg["paths"]
print(json.dumps({
  "raw_incoming_dir": paths["raw_incoming_dir"],
  "raw_archive_dir": paths["raw_archive_dir"],
  "checkpoint_dir": paths["checkpoint_dir"],
  "bronze_dir": paths["bronze_dir"],
}, ensure_ascii=False))
"@
  $raw = Run-PythonInline $projectRoot $code
  if (-not $raw) { Fail "Failed to parse settings.yaml via Python" }
  return ($raw | ConvertFrom-Json)
}

function Read-RequiredFilesFromBronzeTables($projectRoot, $bronzeTablesRel) {
  $code = @"
from pathlib import Path
import yaml, json

p = Path(r"$projectRoot") / r"$bronzeTablesRel"
with open(p, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

files = [t["file"] for t in cfg["tables"]]
print(json.dumps(files, ensure_ascii=False))
"@
  $raw = Run-PythonInline $projectRoot $code
  if (-not $raw) { Fail "Failed to parse bronze_tables.yaml via Python" }
  return ($raw | ConvertFrom-Json)
}

function Get-LatestMarker($incomingDir) {
  if (!(Test-Path $incomingDir)) { return $null }

  $markers = @(Get-ChildItem -Path $incomingDir -File -Filter "_BATCH_*.ready" -ErrorAction SilentlyContinue)
  if ($markers.Length -eq 0) { return $null }

  # latest modified
  return ($markers | Sort-Object LastWriteTime -Descending | Select-Object -First 1)
}

function Parse-BatchIdFromMarkerName($markerName) {
  # _BATCH_batch_YYYYMMDD_HHMM.ready
  if ($markerName -match '^_BATCH_(batch_\d{8}_\d{4})\.ready$') {
    return $matches[1]
  }
  Fail "Invalid marker filename: $markerName"
}

function Ensure-Dir($path) {
  if (!(Test-Path $path)) {
    New-Item -ItemType Directory -Path $path | Out-Null
  }
}

function Move-FileSafe($src, $dst) {
  if (!(Test-Path $src)) { Fail "Source missing: $src" }
  if (Test-Path $dst) { Fail "Destination already exists: $dst" }
  Move-Item -Path $src -Destination $dst
}

function Set-PySparkPythonToVenv($projectRoot) {
  $venvPy = Join-Path $projectRoot "venv\Scripts\python.exe"
  if (!(Test-Path $venvPy)) {
    Fail "UseVenvPython requested but venv python not found: $venvPy"
  }
  $env:PYSPARK_PYTHON = $venvPy
  $env:PYSPARK_DRIVER_PYTHON = $venvPy
  Ok "PYSPARK_PYTHON set to venv: $venvPy"
}

# =========================
# MAIN
# =========================
$projectRoot = Get-ProjectRoot
$settingsFull = Join-Path $projectRoot $SettingsPath
$bronzeTablesFull = Join-Path $projectRoot $BronzeTablesPath

if (!(Test-Path $settingsFull)) { Fail "settings.yaml not found: $settingsFull" }
if (!(Test-Path $bronzeTablesFull)) { Fail "bronze_tables.yaml not found: $bronzeTablesFull" }

Info "Project root      : $projectRoot"
Info "Settings          : $settingsFull"
Info "Bronze tables     : $bronzeTablesFull"

if ($UseVenvPython) {
  Set-PySparkPythonToVenv $projectRoot
}

$paths = Read-PathsFromSettings $projectRoot $SettingsPath
$requiredFiles = Read-RequiredFilesFromBronzeTables $projectRoot $BronzeTablesPath

$incomingDir = Join-Path $projectRoot $paths.raw_incoming_dir
$archiveDir  = Join-Path $projectRoot $paths.raw_archive_dir

Info "Incoming dir      : $incomingDir"
Info "Archive dir       : $archiveDir"
Info "Required files    : $($requiredFiles -join ', ')"

if ($Seed) {
  Info "Running seed_incoming.ps1 ..."
  $seedArgs = @("-ExecutionPolicy", "Bypass", "-File", "scripts\seed_incoming.ps1", "-SettingsPath", $SettingsPath)
  if ($CleanIncoming) { $seedArgs += "-CleanIncoming" }
  $p = Start-Process -FilePath "powershell" -ArgumentList $seedArgs -NoNewWindow -Wait -PassThru
  if ($p.ExitCode -ne 0) { Fail "Seeding failed (exit code $($p.ExitCode))" }
  Ok "Seeding complete."
}

# Detect latest marker (batch)
$marker = Get-LatestMarker $incomingDir
if ($null -eq $marker) {
  Fail "No batch marker found in incoming/. Run seed first or ensure marker exists."
}

$batchId = Parse-BatchIdFromMarkerName $marker.Name
Info "Detected marker   : $($marker.Name)"
Info "Batch ID          : $batchId"

# Validate required files exist before ingest
foreach ($f in $requiredFiles) {
  $p = Join-Path $incomingDir $f
  if (!(Test-Path $p)) {
    Fail "Invalid batch: missing required file in incoming/: $f"
  }
}

Info "Running spark-submit bronze ingest ..."
$sparkArgs = @(
  "spark_jobs\bronze_ingest.py",
  "--settings", $SettingsPath,
  "--bronze-tables", $BronzeTablesPath
)

# Use call operator to preserve exit code
& spark-submit @sparkArgs
$exit = $LASTEXITCODE
if ($exit -ne 0) {
  Fail "Bronze ingest failed (spark-submit exit code $exit). Files remain in incoming/ for retry."
}

Ok "Bronze ingest succeeded."

Info "Archiving processed batch files ..."

$batchArchiveDir = Join-Path $archiveDir $batchId
Ensure-Dir $archiveDir
Ensure-Dir $batchArchiveDir

# Move CSV files
foreach ($f in $requiredFiles) {
  $src = Join-Path $incomingDir $f
  $dst = Join-Path $batchArchiveDir $f
  Move-FileSafe $src $dst
}

$markerDst = Join-Path $batchArchiveDir $marker.Name
Move-FileSafe $marker.FullName $markerDst

Ok "Archive complete: $batchArchiveDir"
Ok "incoming/ is now clean for next batch."

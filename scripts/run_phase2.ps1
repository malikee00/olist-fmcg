param(
  [string]$SettingsPath = "configs/settings.yaml",
  [string]$BronzeTablesPath = "configs/bronze_tables.yaml",
  [switch]$Seed,
  [switch]$CleanIncoming,
  [switch]$UseVenvPython
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Fail($msg) { Write-Host "[ERROR] $msg" -ForegroundColor Red; exit 1 }
function Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "[OK] $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }

function Get-ProjectRoot {
  return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Run-PythonInline($code) {
  $tmpPy = Join-Path $env:TEMP ("py_" + [Guid]::NewGuid().ToString() + ".py")
  try {
    $code | Set-Content -Path $tmpPy -Encoding utf8
    return (python $tmpPy)
  } finally {
    if (Test-Path $tmpPy) { Remove-Item $tmpPy -Force }
  }
}

function Read-PathsFromSettings($projectRoot, $settingsRel) {
  $code = @"
from pathlib import Path
import yaml, json
p = Path(r"$projectRoot") / r"$settingsRel"
with open(p, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)
paths = cfg["paths"]
print(json.dumps({
  "raw_incoming_dir": paths["raw_incoming_dir"],
  "raw_archive_dir": paths["raw_archive_dir"],
  "checkpoint_dir": paths["checkpoint_dir"],
  "bronze_dir": paths["bronze_dir"]
}, ensure_ascii=False))
"@
  $raw = Run-PythonInline $code
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
  $raw = Run-PythonInline $code
  if (-not $raw) { Fail "Failed to parse bronze_tables.yaml via Python" }
  return ($raw | ConvertFrom-Json)
}

function Get-LatestMarker($incomingDir) {
  if (!(Test-Path $incomingDir)) { return $null }
  $markers = @(Get-ChildItem -Path $incomingDir -File -Filter "_BATCH_*.ready" -ErrorAction SilentlyContinue)
  if ($markers.Length -eq 0) { return $null }
  return ($markers | Sort-Object LastWriteTime -Descending | Select-Object -First 1)
}

function Parse-BatchIdFromMarkerName($markerName) {
  if ($markerName -match '^_BATCH_(batch_\d{8}_\d{4})\.ready$') { return $matches[1] }
  Fail "Invalid marker filename: $markerName"
}

function Ensure-Dir($path) {
  if (!(Test-Path $path)) { New-Item -ItemType Directory -Path $path | Out-Null }
}

function Set-PySparkPythonToVenv($projectRoot) {
  $venvPy = Join-Path $projectRoot "venv\Scripts\python.exe"
  if (!(Test-Path $venvPy)) { Fail "venv python not found: $venvPy" }
  $env:PYSPARK_PYTHON = $venvPy
  $env:PYSPARK_DRIVER_PYTHON = $venvPy
  Ok "PYSPARK_PYTHON set to venv: $venvPy"
}

function Try-MoveIfExists($src, $dst) {
  if (!(Test-Path $src)) { return $false }
  if (Test-Path $dst) { Fail "Destination already exists: $dst" }
  Move-Item -Path $src -Destination $dst
  return $true
}

# =========================
# MAIN
# =========================
$projectRoot = Get-ProjectRoot
$settingsFull = Join-Path $projectRoot $SettingsPath
$bronzeTablesFull = Join-Path $projectRoot $BronzeTablesPath

Info "Project root      : $projectRoot"
Info "Settings          : $settingsFull"
Info "Bronze tables     : $bronzeTablesFull"

if (!(Test-Path $settingsFull)) { Fail "settings.yaml not found: $settingsFull" }
if (!(Test-Path $bronzeTablesFull)) { Fail "bronze_tables.yaml not found: $bronzeTablesFull" }

if ($UseVenvPython) { Set-PySparkPythonToVenv $projectRoot }

$paths = Read-PathsFromSettings $projectRoot $SettingsPath
$requiredFiles = Read-RequiredFilesFromBronzeTables $projectRoot $BronzeTablesPath

$incomingDir = Join-Path $projectRoot $paths.raw_incoming_dir
$archiveDir  = Join-Path $projectRoot $paths.raw_archive_dir

Info "Incoming dir      : $incomingDir"
Info "Archive dir       : $archiveDir"
Info "Required files    : $($requiredFiles -join ', ')"

# Optional: Seed
if ($Seed) {
  Info "Running seed_incoming.ps1 ..."
  $seedArgs = @("-ExecutionPolicy", "Bypass", "-File", "scripts\seed_incoming.ps1", "-SettingsPath", $SettingsPath)
  if ($CleanIncoming) { $seedArgs += "-CleanIncoming" }
  $p = Start-Process -FilePath "powershell" -ArgumentList $seedArgs -NoNewWindow -Wait -PassThru
  if ($p.ExitCode -ne 0) { Fail "Seeding failed (exit code $($p.ExitCode))" }
  Ok "Seeding complete."
}

# 1) Detect batch
$marker = Get-LatestMarker $incomingDir

# 2) NO-OP success if no batch (Airflow-safe)
if ($null -eq $marker) {
  Ok "NO-OP: no batch marker found in incoming/. Nothing to process."
  exit 0
}

$batchId = Parse-BatchIdFromMarkerName $marker.Name
Info "Detected marker   : $($marker.Name)"
Info "Batch ID          : $batchId"

# Atomic batch gate: required files must exist
foreach ($f in $requiredFiles) {
  $p = Join-Path $incomingDir $f
  if (!(Test-Path $p)) { Fail "Invalid batch: missing required file in incoming/: $f" }
}

# 3) Ingest
Info "Running spark-submit bronze ingest ..."
& spark-submit "spark_jobs\bronze_ingest.py" --settings $SettingsPath --bronze-tables $BronzeTablesPath
$ingestExit = $LASTEXITCODE
if ($ingestExit -ne 0) { Fail "Bronze ingest failed (exit code $ingestExit). Files remain in incoming/ for retry." }
Ok "Bronze ingest succeeded."

# 4) Validate
Info "Running spark-submit bronze validate ..."
& spark-submit "spark_jobs\bronze_validate.py" --settings $SettingsPath --bronze-tables $BronzeTablesPath --batch-id $batchId
$valExit = $LASTEXITCODE
if ($valExit -ne 0) { Fail "Bronze validation failed (exit code $valExit). Files remain in incoming/ for investigation/retry." }
Ok "Bronze validation PASSED."

# 5/6) Archive after validate PASS
Info "Archiving processed batch files ..."
Ensure-Dir $archiveDir
$batchArchiveDir = Join-Path $archiveDir $batchId
Ensure-Dir $batchArchiveDir

$moveCount = 0
foreach ($f in $requiredFiles) {
  $src = Join-Path $incomingDir $f
  $dst = Join-Path $batchArchiveDir $f
  if (Try-MoveIfExists $src $dst) { $moveCount++ }
}

$markerDst = Join-Path $batchArchiveDir $marker.Name
if (Try-MoveIfExists $marker.FullName $markerDst) { $moveCount++ }

if ($moveCount -eq 0) { Warn "Nothing moved. Possibly already archived earlier." }
else { Ok "Archived $moveCount file(s) into: $batchArchiveDir" }

Ok "Phase 2 runner complete."
exit 0
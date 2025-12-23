param(
  [string]$SettingsPath = "configs/settings.yaml",
  [switch]$CleanIncoming,
  [switch]$NoMarker
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

function Get-RawIncomingDirFromPython($projectRoot, $settingsPath) {
  $tmpPy = Join-Path $env:TEMP ("read_settings_" + [Guid]::NewGuid().ToString() + ".py")

  $py = @"
from pathlib import Path
import yaml

settings_path = Path(r"$projectRoot") / r"$settingsPath"
with open(settings_path, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

print(cfg["paths"]["raw_incoming_dir"])
"@

  $py | Set-Content -Path $tmpPy -Encoding utf8

  try {
    $result = python $tmpPy
    if (-not $result) {
      Fail "Failed to read raw_incoming_dir from settings.yaml via Python"
    }
    return $result.Trim()
  }
  finally {
    if (Test-Path $tmpPy) { Remove-Item $tmpPy -Force }
  }
}

function Make-BatchId {
  return "batch_{0}" -f (Get-Date -Format "yyyyMMdd_HHmm")
}

function Ensure-Dir($path) {
  if (!(Test-Path $path)) {
    New-Item -ItemType Directory -Path $path | Out-Null
  }
}

function Remove-AllFiles($dir) {
  if (!(Test-Path $dir)) { return }
  Get-ChildItem -Path $dir -File | Remove-Item -Force
}

# =========================
# MAIN
# =========================
$projectRoot = Get-ProjectRoot
$settingsFullPath = Join-Path $projectRoot $SettingsPath

if (!(Test-Path $settingsFullPath)) {
  Fail "settings.yaml not found: $settingsFullPath"
}

$rawIncomingRel = Get-RawIncomingDirFromPython $projectRoot $SettingsPath

$referenceDir = Join-Path $projectRoot "data\raw\olist\reference"
$incomingDir  = Join-Path $projectRoot $rawIncomingRel

Info "Project root : $projectRoot"
Info "Settings     : $settingsFullPath"
Info "Reference    : $referenceDir"
Info "Incoming     : $incomingDir"

if (!(Test-Path $referenceDir)) {
  Fail "Reference directory not found: $referenceDir`nPut Olist CSV files here before seeding."
}

Ensure-Dir $incomingDir

$requiredFiles = @(
  "olist_orders_dataset.csv",
  "olist_order_items_dataset.csv",
  "olist_products_dataset.csv",
  "olist_customers_dataset.csv",
  "olist_order_payments_dataset.csv"
)

$missing = @()
foreach ($f in $requiredFiles) {
  if (!(Test-Path (Join-Path $referenceDir $f))) {
    $missing += $f
  }
}

if ($missing.Count -gt 0) {
  Fail ("Missing required file(s) in reference/: " + ($missing -join ", "))
}

if ($CleanIncoming) {
  Info "Cleaning incoming/ ..."
  Remove-AllFiles $incomingDir
}

$existing = @(Get-ChildItem -Path $incomingDir -File -ErrorAction SilentlyContinue)
if ($existing.Length -gt 0) {
  Fail "incoming/ is not empty. Use -CleanIncoming to force cleanup."
}

$batchId = Make-BatchId
Info "Generated batch_id: $batchId"

foreach ($f in $requiredFiles) {
  Copy-Item `
    -Path (Join-Path $referenceDir $f) `
    -Destination (Join-Path $incomingDir $f) `
    -Force
}

Ok "Copied CSV files into incoming/"

if (-not $NoMarker) {
  $markerName = "_BATCH_${batchId}.ready"
  $markerPath = Join-Path $incomingDir $markerName

  @(
    "batch_id=$batchId"
    "created_at=$(Get-Date -Format o)"
    "files=$($requiredFiles -join ',')"
  ) | Set-Content -Path $markerPath -Encoding utf8

  Ok "Marker created: $markerName"
} else {
  Info "Marker creation skipped (-NoMarker)"
}

Write-Host ""
Write-Host "=== Seed Summary ===" -ForegroundColor Yellow
Write-Host "batch_id : $batchId"
Write-Host "incoming : $incomingDir"
Write-Host "files    : $($requiredFiles -join ', ')"
Write-Host ""

Ok "Seed incoming complete."

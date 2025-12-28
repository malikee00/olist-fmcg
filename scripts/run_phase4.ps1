param(
  [switch]$UseVenvPython,
  [switch]$Force,
  [string]$ProjectRoot = (Resolve-Path ".").Path
)

$ErrorActionPreference = "Stop"

function Read-Json([string]$Path) {
  if (!(Test-Path $Path)) { return $null }
  $raw = Get-Content $Path -Raw
  if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
  return $raw | ConvertFrom-Json
}

function Write-Json([string]$Path, $Obj) {
  $dir = Split-Path $Path -Parent
  if (!(Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
  ($Obj | ConvertTo-Json -Depth 10) | Set-Content -Encoding UTF8 $Path
}

Write-Host "[INFO] Project root : $ProjectRoot"

$settings = Join-Path $ProjectRoot "configs\settings.yaml"
$kpis     = Join-Path $ProjectRoot "configs\gold_kpis.yaml"

$pythonExe = "python"
if ($UseVenvPython) {
  $venvPy = Join-Path $ProjectRoot "venv\Scripts\python.exe"
  if (Test-Path $venvPy) { $pythonExe = $venvPy }
}
Write-Host "[INFO] Python      : $pythonExe"

$bronzeCheckpoint = Join-Path $ProjectRoot "data\checkpoints\bronze_ingest_state.json"
$silverCheckpoint = Join-Path $ProjectRoot "data\checkpoints\silver_transform_state.json"
$goldCheckpoint   = Join-Path $ProjectRoot "data\checkpoints\gold_aggregate_state.json"

# 1) read bronze last_batch_id
$cpBronze = Read-Json $bronzeCheckpoint
if ($null -eq $cpBronze -or [string]::IsNullOrWhiteSpace($cpBronze.last_batch_id)) {
  Write-Host "[NO-OP] No last_batch_id in bronze checkpoint. Phase 4 no-op success."
  exit 0
}
$batchId = $cpBronze.last_batch_id
Write-Host "[INFO] last_batch_id : $batchId"

# 2) Gate
$cpSilver = Read-Json $silverCheckpoint
if (-not $Force) {
  if ($null -eq $cpSilver -or $cpSilver.last_processed_batch_id -ne $batchId) {
    throw "[GATE] Silver not ready for this batch. Run Phase 3 first."
  }
} else {
  Write-Host "[FORCE] Skipping silver gate (debug mode)."
}

# 3) Idempotency
$cpGold = Read-Json $goldCheckpoint
if (-not $Force) {
  if ($null -ne $cpGold -and $cpGold.last_processed_batch_id -eq $batchId) {
    Write-Host "[NO-OP] Gold already processed batch_id=$batchId. Phase 4 no-op success."
    exit 0
  }
} else {
  Write-Host "[FORCE] Re-running Gold for batch_id=$batchId"
}

# 4) Run gold aggregation
Write-Host "[RUN] gold_aggregate.py"
& $pythonExe (Join-Path $ProjectRoot "spark_jobs\gold_aggregate.py") `
  --settings $settings `
  --kpis $kpis `
  --batch_id $batchId
if ($LASTEXITCODE -ne 0) { throw "gold_aggregate failed" }

# 5) Run gold validation
Write-Host "[RUN] gold_validate.py"
& $pythonExe (Join-Path $ProjectRoot "spark_jobs\gold_validate.py") `
  --settings $settings `
  --kpis $kpis `
  --batch_id $batchId
if ($LASTEXITCODE -ne 0) { throw "gold_validate failed" }

Write-Host "[OK] Phase 4 completed successfully for batch_id=$batchId"
exit 0

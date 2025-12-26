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

Write-Host "[INFO] Project root      : $ProjectRoot"

$config = Join-Path $ProjectRoot "configs\silver_models.yaml"
$bronzeCheckpoint = Join-Path $ProjectRoot "data\checkpoints\bronze_ingest_state.json"
$silverCheckpoint = Join-Path $ProjectRoot "data\checkpoints\silver_transform_state.json"

# Python
$pythonExe = "python"
if ($UseVenvPython) {
  $venvPy = Join-Path $ProjectRoot "venv\Scripts\python.exe"
  if (Test-Path $venvPy) { $pythonExe = $venvPy }
}
Write-Host "[INFO] Python            : $pythonExe"

# ---- get batch context
$cpBronze = Read-Json $bronzeCheckpoint
if ($null -eq $cpBronze -or [string]::IsNullOrWhiteSpace($cpBronze.last_batch_id)) {
  Write-Host "[NO-OP] No last_batch_id in bronze checkpoint."
  exit 0
}

$batchId = $cpBronze.last_batch_id
Write-Host "[INFO] last_batch_id     : $batchId"

# ---- idempotency guard
$cpSilver = Read-Json $silverCheckpoint
if (-not $Force) {
  if ($null -ne $cpSilver -and $cpSilver.last_processed_batch_id -eq $batchId) {
    Write-Host "[NO-OP] Batch already processed in Silver."
    Write-Host "        Use -Force to re-run transform safely."
    exit 0
  }
} else {
  Write-Host "[FORCE] Re-running Silver transform for batch_id=$batchId"
}

# ---- Run transform
Write-Host "[RUN] silver_transform.py"
& $pythonExe (Join-Path $ProjectRoot "spark_jobs\silver_transform.py") `
  --config $config `
  --batch_id $batchId

if ($LASTEXITCODE -ne 0) {
  throw "silver_transform failed"
}

# ---- Validate
Write-Host "[RUN] silver_validate.py"
& $pythonExe (Join-Path $ProjectRoot "spark_jobs\silver_validate.py") `
  --config $config `
  --batch_id $batchId

if ($LASTEXITCODE -ne 0) {
  throw "silver_validate failed"
}

# ---- Update checkpoint ONLY if not force OR if force succeeded
$updated = @{
  last_processed_batch_id = $batchId
  updated_at = (Get-Date).ToString("s")
  forced = $Force.IsPresent
}

Write-Json $silverCheckpoint $updated
Write-Host "[OK] Updated silver checkpoint."

Write-Host "[OK] Phase 3 completed successfully for batch_id=$batchId"
exit 0

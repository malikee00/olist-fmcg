# ================================
# Create ML Olist Project Structure
# ================================

$base = "ml_olist_project"

$folders = @(
    "$base/pipelines_spark",
    "$base/features",
    "$base/ml",
    "$base/app",
    "$base/demo_ui",
    "$base/ops"
)

foreach ($folder in $folders) {
    if (-Not (Test-Path $folder)) {
        New-Item -ItemType Directory -Path $folder | Out-Null
        Write-Host "Created: $folder"
    } else {
        Write-Host "Exists:  $folder"
    }
}

Write-Host "[DONE] ML Olist project structure created successfully."

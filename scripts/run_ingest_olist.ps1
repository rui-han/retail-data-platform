# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

$ErrorActionPreference = "Stop"

# Usage:
#   ./scripts/run_ingest_olist.ps1
#   $env:DATASET="olistbr/brazilian-ecommerce"; $env:BUCKET="raw"; $env:PREFIX="olist"; ./scripts/run_ingest_olist.ps1

Get-Content .env | ForEach-Object {
  if ($_ -match "^(.*?)=(.*)$") {
    [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2])
  }
}

$dataset = if ($env:DATASET) { $env:DATASET } else { "olistbr/brazilian-ecommerce" }
$bucket = if ($env:BUCKET) { $env:BUCKET } else { "raw" }
$prefix = if ($env:PREFIX) { $env:PREFIX } else { "olist" }
$timeoutSec = if ($env:TIMEOUT_SEC) { $env:TIMEOUT_SEC } else { "300" }

$requiredEnvVars = @(
    "KAGGLE_USERNAME",
    "KAGGLE_KEY",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY"
)

foreach ($envName in $requiredEnvVars) {
    $value = [Environment]::GetEnvironmentVariable($envName)
    if ([string]::IsNullOrWhiteSpace($value)) {
        Write-Error "ERROR: missing required env var: $envName"
        exit 1
    }
}

Write-Host "Running Olist ingest with:"
Write-Host "  DATASET=$dataset"
Write-Host "  BUCKET=$bucket"
Write-Host "  PREFIX=$prefix"
Write-Host "  TIMEOUT_SEC=$timeoutSec"

python jobs/ingestion/ingest_olist.py `
  --dataset "$dataset" `
  --bucket "$bucket" `
  --prefix "$prefix" `
  --timeout-sec "$timeoutSec"
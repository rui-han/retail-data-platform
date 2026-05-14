# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

$ErrorActionPreference = "Stop"

# Load .env into the current process environment
if (Test-Path .env) {
  Get-Content .env | ForEach-Object {
    if ($_ -match "^\s*([^#][^=]*)=(.*)$") {
      [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
    }
  }
} else {
  Write-Error "ERROR: .env file not found. Copy .env.example to .env and fill in your credentials."
  exit 1
}

$dataset   = if ($env:DATASET)      { $env:DATASET }      else { "olistbr/brazilian-ecommerce" }
$bucket    = if ($env:BUCKET)       { $env:BUCKET }       else { "raw" }
$prefix    = if ($env:PREFIX)       { $env:PREFIX }       else { "olist" }
$timeoutSec = if ($env:TIMEOUT_SEC) { $env:TIMEOUT_SEC }  else { "300" }

$requiredEnvVars = @("KAGGLE_USERNAME", "KAGGLE_KEY", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")
foreach ($envName in $requiredEnvVars) {
  if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($envName))) {
    Write-Error "ERROR: missing required env var: $envName"
    exit 1
  }
}

Write-Host "Running Kaggle upload:"
Write-Host "  DATASET=$dataset  BUCKET=$bucket  PREFIX=$prefix  TIMEOUT_SEC=$timeoutSec"

python jobs/ingestion/kaggle_upload.py `
  --dataset "$dataset" `
  --bucket  "$bucket" `
  --prefix  "$prefix" `
  --timeout-sec "$timeoutSec"
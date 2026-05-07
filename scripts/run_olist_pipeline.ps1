# powershell -ExecutionPolicy Bypass -File .\scripts\run_olist_pipeline.ps1

param(
  [string]$Container = "spark",
  [string]$Master    = "local[*]",
  [string]$Packages  = "org.apache.hadoop:hadoop-aws:3.3.4"
)

$jobs = @(
  "jobs/ingestion/bronze_ingest_olist.py",
  "jobs/etl/etl_orders.py",
  "jobs/transformation/silver_build_model.py",
  "jobs/transformation/gold_build_marts.py"
)

foreach ($job in $jobs) {
  Write-Host "[RUN] $job" -ForegroundColor Cyan
  docker exec $Container /opt/spark/bin/spark-submit `
    --master $Master `
    --packages $Packages `
    $job

  if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] $job exited with code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
  }

  Write-Host "[OK] $job" -ForegroundColor Green
}

Write-Host "Pipeline completed successfully." -ForegroundColor Green
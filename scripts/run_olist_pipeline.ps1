# powershell -ExecutionPolicy Bypass -File .\scripts\run_olist_pipeline.ps1


param(
  [string]$Container = "spark",
  [string]$Master = "local[*]",
  [string]$Packages = "org.apache.hadoop:hadoop-aws:3.3.4"
)

$jobs = @(
  "jobs/bronze_ingest_olist.py",
  "jobs/etl_orders.py",
  "jobs/silver_build_model.py",
  "jobs/gold_build_marts.py",
  "jobs/build_order_analytics_mart.py",
  "jobs/build_order_kpis.py"
)

foreach ($job in $jobs) {
  Write-Host "[RUN] $job" -ForegroundColor Cyan
  docker exec $Container /opt/spark/bin/spark-submit --master $Master --packages $Packages $job

  if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] $job" -ForegroundColor Red
    exit $LASTEXITCODE
  }

  Write-Host "[OK] $job" -ForegroundColor Green
}

Write-Host "Pipeline completed successfully." -ForegroundColor Green
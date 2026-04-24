#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${1:-spark}"
MASTER="${SPARK_MASTER:-local[*]}"
PACKAGES="${SPARK_PACKAGES:-org.apache.hadoop:hadoop-aws:3.3.4}"

jobs=(
  "jobs/bronze_ingest_olist.py"
  "jobs/etl_orders.py"
  "jobs/silver_build_model.py"
  "jobs/gold_build_marts.py"
  "jobs/build_order_analytics_mart.py"
  "jobs/build_order_kpis.py"
)

for job in "${jobs[@]}"; do
  echo "[RUN] ${job}"
  docker exec "${CONTAINER}" /opt/spark/bin/spark-submit \
    --master "${MASTER}" \
    --packages "${PACKAGES}" \
    "${job}"
  echo "[OK] ${job}"
done

echo "Pipeline completed successfully."

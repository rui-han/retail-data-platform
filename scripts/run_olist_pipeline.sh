#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${1:-spark}"
MASTER="${SPARK_MASTER:-local[*]}"
PACKAGES="${SPARK_PACKAGES:-org.apache.hadoop:hadoop-aws:3.3.4}"

jobs=(
  "jobs/ingestion/bronze_ingest_olist.py"
  "jobs/etl/etl_orders.py"
  "jobs/transformation/silver_build_model.py"
  "jobs/transformation/gold_build_marts.py"
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
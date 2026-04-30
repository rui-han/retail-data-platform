#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/run_ingest_olist.sh
#   DATASET=olistbr/brazilian-ecommerce BUCKET=raw PREFIX=olist ./scripts/run_ingest_olist.sh

DATASET="${DATASET:-olistbr/brazilian-ecommerce}"
BUCKET="${BUCKET:-raw}"
PREFIX="${PREFIX:-olist}"
TIMEOUT_SEC="${TIMEOUT_SEC:-300}"

required_env_vars=(
  "KAGGLE_USERNAME"
  "KAGGLE_KEY"
  "S3_ENDPOINT"
  "S3_ACCESS_KEY"
  "S3_SECRET_KEY"
)

for env_name in "${required_env_vars[@]}"; do
  if [[ -z "${!env_name:-}" ]]; then
    echo "ERROR: missing required env var: ${env_name}" >&2
    exit 1
  fi
done

echo "Running Olist ingest with:"
echo "  DATASET=${DATASET}"
echo "  BUCKET=${BUCKET}"
echo "  PREFIX=${PREFIX}"
echo "  TIMEOUT_SEC=${TIMEOUT_SEC}"

python jobs/ingest/ingest_olist.py \
  --dataset "${DATASET}" \
  --bucket "${BUCKET}" \
  --prefix "${PREFIX}" \
  --timeout-sec "${TIMEOUT_SEC}"
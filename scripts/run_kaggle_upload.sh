#!/usr/bin/env bash
set -euo pipefail

# ── Load .env if present ──────────────────────────────────────────────────────
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source <(grep -E '^\s*[^#][^=]*=.*' .env)
  set +a
else
  echo "ERROR: .env file not found. Copy .env.example to .env and fill in your credentials." >&2
  exit 1
fi

# ── Validate required env vars ────────────────────────────────────────────────
DATASET="${DATASET:-olistbr/brazilian-ecommerce}"
BUCKET="${BUCKET:-raw}"
PREFIX="${PREFIX:-olist}"
TIMEOUT_SEC="${TIMEOUT_SEC:-300}"

required_env_vars=(KAGGLE_USERNAME KAGGLE_KEY S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY)
for env_name in "${required_env_vars[@]}"; do
  if [[ -z "${!env_name:-}" ]]; then
    echo "ERROR: missing required env var: ${env_name}" >&2
    exit 1
  fi
done

echo "Running Kaggle upload: DATASET=${DATASET} BUCKET=${BUCKET} PREFIX=${PREFIX}"

python jobs/ingestion/kaggle_upload.py \
  --dataset     "${DATASET}" \
  --bucket      "${BUCKET}" \
  --prefix      "${PREFIX}" \
  --timeout-sec "${TIMEOUT_SEC}"
from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import boto3
import requests
from botocore.exceptions import ClientError

from core.config import _require_env
from core.logger import logger


_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_RETRY_DELAY = 5   # seconds between attempts


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Olist dataset from Kaggle and upload CSV files to MinIO/S3."
    )
    parser.add_argument("--dataset",     default="olistbr/brazilian-ecommerce")
    parser.add_argument("--bucket",      default="raw")
    parser.add_argument("--prefix",      default="olist")
    parser.add_argument("--timeout-sec", type=int, default=300)
    return parser.parse_args()


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_require_env("S3_ENDPOINT"),
        aws_access_key_id=_require_env("S3_ACCESS_KEY"),
        aws_secret_access_key=_require_env("S3_SECRET_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def ensure_bucket(s3_client, bucket: str):
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError:
        logger.info("Bucket %s not found – creating.", bucket)
        s3_client.create_bucket(Bucket=bucket)


def download_kaggle_dataset_zip(dataset: str, timeout_sec: int, dest_dir: Path) -> Path:
    """Download the Kaggle dataset zip, retrying on transient network errors.

    Retries up to _DOWNLOAD_MAX_RETRIES times with a fixed delay between
    attempts. HTTP 4xx errors (bad credentials, dataset not found) are not
    retried — they indicate a configuration problem that a retry won't fix.
    """
    kaggle_username = _require_env("KAGGLE_USERNAME")
    kaggle_key      = _require_env("KAGGLE_KEY")
    url = f"https://www.kaggle.com/api/v1/datasets/download/{quote(dataset, safe='/')}"
    zip_path = dest_dir / "dataset.zip"

    for attempt in range(1, _DOWNLOAD_MAX_RETRIES + 1):
        logger.info("Downloading Kaggle dataset from %s (attempt %d/%d)",
                    url, attempt, _DOWNLOAD_MAX_RETRIES)
        try:
            response = requests.get(
                url,
                auth=(kaggle_username, kaggle_key),
                stream=True,
                timeout=timeout_sec,
            )
            if 400 <= response.status_code < 500:
                response.raise_for_status()

            response.raise_for_status()

            with zip_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            logger.info("Downloaded zip → %s (%.1f MB)",
                        zip_path, zip_path.stat().st_size / 1e6)
            return zip_path

        except requests.exceptions.HTTPError:
            raise
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as exc:
            if attempt == _DOWNLOAD_MAX_RETRIES:
                raise RuntimeError(
                    f"Kaggle download failed after {_DOWNLOAD_MAX_RETRIES} attempts: {exc}"
                ) from exc
            logger.warning(
                "Download attempt %d failed (%s). Retrying in %ds…",
                attempt, exc, _DOWNLOAD_RETRY_DELAY,
            )
            if zip_path.exists():
                zip_path.unlink()
            time.sleep(_DOWNLOAD_RETRY_DELAY)

    raise RuntimeError("Unexpected exit from download retry loop.")


def extract_csv_files(zip_path: Path) -> list[Path]:
    extract_dir = zip_path.parent / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(extract_dir)

    csv_files = sorted(extract_dir.rglob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found inside {zip_path}.")

    logger.info("Extracted %d CSV files", len(csv_files))
    return csv_files


def upload_files(s3_client, csv_files: list[Path], bucket: str, prefix: str, dataset: str):
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    uploaded_objects = []

    for file_path in csv_files:
        archive_key = f"{prefix}/archive/{run_ts}/{file_path.name}"
        logger.info("Uploading %s → s3://%s/%s", file_path.name, bucket, archive_key)
        s3_client.upload_file(str(file_path), bucket, archive_key)

        latest_key = f"{prefix}/latest/{file_path.name}"
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": archive_key},
            Key=latest_key,
        )

        uploaded_objects.append({
            "file_name":   file_path.name,
            "archive_key": archive_key,
            "latest_key":  latest_key,
            "size_bytes":  file_path.stat().st_size,
        })

    manifest = {
        "source":     f"kaggle:{dataset}",
        "run_ts_utc": run_ts,
        "file_count": len(uploaded_objects),
        "files":      uploaded_objects,
    }

    manifest_body = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    for key in (
        f"{prefix}/archive/{run_ts}/_manifest.json",
        f"{prefix}/latest/_manifest.json",
    ):
        s3_client.put_object(
            Bucket=bucket, Key=key,
            Body=manifest_body, ContentType="application/json",
        )

    logger.info("Manifest written for run %s (%d files)", run_ts, len(uploaded_objects))


def run():
    args = parse_args()
    s3_client = build_s3_client()
    ensure_bucket(s3_client, args.bucket)

    with tempfile.TemporaryDirectory(prefix="olist_ingest_") as tmp:
        tmp_path  = Path(tmp)
        zip_path  = download_kaggle_dataset_zip(args.dataset, args.timeout_sec, tmp_path)
        csv_files = extract_csv_files(zip_path)
        upload_files(
            s3_client=s3_client,
            csv_files=csv_files,
            bucket=args.bucket,
            prefix=args.prefix,
            dataset=args.dataset,
        )

    logger.info("Ingest completed successfully.")


if __name__ == "__main__":
    run()
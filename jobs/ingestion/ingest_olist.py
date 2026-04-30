import argparse
import json
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import boto3
import requests
from botocore.exceptions import ClientError

from core.logger import logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Olist dataset from Kaggle and upload CSV files to MinIO/S3."
    )
    parser.add_argument(
        "--dataset",
        default="olistbr/brazilian-ecommerce",
        help="Kaggle dataset slug in the format <owner>/<dataset>."
    )
    parser.add_argument(
        "--bucket",
        default="raw",
        help="Target S3/MinIO bucket."
    )
    parser.add_argument(
        "--prefix",
        default="olist",
        help="Target object key prefix in bucket."
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=300,
        help="HTTP timeout in seconds for Kaggle download request."
    )
    return parser.parse_args()


def get_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Environment variable {name} is required.")
    return value


def build_s3_client():
    endpoint_url = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("S3_ACCESS_KEY", "admin")
    secret_key = os.getenv("S3_SECRET_KEY", "password123")
    region = os.getenv("AWS_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )


def ensure_bucket(s3_client, bucket: str):
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError:
        logger.info("Bucket %s does not exist; creating it.", bucket)
        s3_client.create_bucket(Bucket=bucket)


def download_kaggle_dataset_zip(dataset: str, timeout_sec: int) -> Path:
    kaggle_username = get_env("KAGGLE_USERNAME")
    kaggle_key = get_env("KAGGLE_KEY")
    encoded_dataset = quote(dataset, safe="/")
    url = f"https://www.kaggle.com/api/v1/datasets/download/{encoded_dataset}"

    logger.info("Downloading Kaggle dataset from %s", url)
    response = requests.get(
        url,
        auth=(kaggle_username, kaggle_key),
        stream=True,
        timeout=timeout_sec
    )
    response.raise_for_status()

    temp_dir = Path(tempfile.mkdtemp(prefix="olist_ingest_"))
    zip_path = temp_dir / "dataset.zip"
    with zip_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    logger.info("Downloaded zip to %s", zip_path)
    return zip_path


def extract_csv_files(zip_path: Path) -> list[Path]:
    extract_dir = zip_path.parent / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(extract_dir)

    csv_files = sorted(extract_dir.rglob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found inside {zip_path}.")

    logger.info("Extracted %s CSV files", len(csv_files))
    return csv_files


def upload_files(s3_client, csv_files: list[Path], bucket: str, prefix: str, dataset: str):
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    uploaded_objects = []

    for file_path in csv_files:
        object_key = f"{prefix}/{run_ts}/{file_path.name}"
        logger.info("Uploading %s to s3://%s/%s",
                    file_path.name, bucket, object_key)
        s3_client.upload_file(str(file_path), bucket, object_key)
        uploaded_objects.append(
            {
                "file_name": file_path.name,
                "object_key": object_key,
                "size_bytes": file_path.stat().st_size
            }
        )

    manifest = {
        "source": f"kaggle:{dataset}",
        "run_ts_utc": run_ts,
        "file_count": len(uploaded_objects),
        "files": uploaded_objects
    }

    manifest_key = f"{prefix}/{run_ts}/_manifest.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, ensure_ascii=False,
                        indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    logger.info("Uploaded manifest to s3://%s/%s", bucket, manifest_key)


def run():
    args = parse_args()
    s3_client = build_s3_client()
    ensure_bucket(s3_client, args.bucket)

    zip_path = download_kaggle_dataset_zip(
        dataset=args.dataset,
        timeout_sec=args.timeout_sec
    )
    csv_files = extract_csv_files(zip_path)
    upload_files(
        s3_client=s3_client,
        csv_files=csv_files,
        bucket=args.bucket,
        prefix=args.prefix,
        dataset=args.dataset
    )

    logger.info("Ingest completed successfully.")


if __name__ == "__main__":
    run()

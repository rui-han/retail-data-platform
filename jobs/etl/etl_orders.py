import argparse

from core.spark_session import get_spark
from core.logger import logger


def parse_args():
    parser = argparse.ArgumentParser(description="ETL job for orders dataset.")
    parser.add_argument(
        "--input-path",
        default="s3a://raw/olist_orders_dataset.csv",
        help="Input CSV path (local/S3A)."
    )
    parser.add_argument(
        "--output-path",
        default="s3a://clean/orders",
        help="Output Parquet path (local/S3A)."
    )
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=["overwrite", "append", "ignore", "error", "errorifexists"],
        help="Spark write mode for output dataset."
    )
    return parser.parse_args()


def run(input_path: str, output_path: str, write_mode: str):
    spark = get_spark("etl_orders")

    logger.info("Loading raw orders from %s", input_path)

    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    required_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
    ]

    missing_columns = [
        col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    df_clean = df.dropna(subset=required_columns).cache()

    raw_count = df.count()
    clean_count = df_clean.count()
    dropped_count = raw_count - clean_count
    logger.info("raw count = %s", raw_count)
    logger.info("clean count = %s", clean_count)
    logger.info("dropped rows = %s", dropped_count)

    logger.info("Writing cleaned orders to %s with mode=%s",
                output_path, write_mode)
    df_clean.write.mode(write_mode).parquet(output_path)
    df_clean.unpersist()

    logger.info("ETL for orders completed successfully.")


if __name__ == "__main__":
    args = parse_args()
    run(args.input_path, args.output_path, args.write_mode)

"""
etl_orders.py — Data-quality gate for the orders table.

Reads from Bronze, validates primary key uniqueness and required fields,
writes clean rows to Silver staging and invalid rows to a quarantine table.
This job must run after bronze_ingest_olist and before silver_build_model.

Silver outputs:
  silver/stg_orders            — valid, deduplicated orders
  silver/stg_orders_quarantine — rows that failed validation
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from core.config import load_storage_config
from core.io import write_parquet
from core.logger import logger
from core.spark_session import get_spark


REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
]


def run():
    spark = get_spark("etl_orders")
    storage = load_storage_config()

    logger.info("Reading bronze orders")
    df = spark.read.parquet(storage.bronze_path("orders"))
    logger.info("Bronze orders: %d rows", df.count())

    # ── 1. Check required columns exist ──────────────────────────────────
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Bronze orders table is missing required columns: {missing}")

    # ── 2. Tag rows that fail validation ─────────────────────────────────
    null_filter = F.lit(False)
    for col in REQUIRED_COLUMNS:
        null_filter = null_filter | F.col(col).isNull()

    # Duplicate order_ids: keep first occurrence, flag the rest.
    window = (
        Window
        .partitionBy("order_id")
        .orderBy("order_purchase_timestamp")
    )
    df_ranked = df.withColumn("_row_num", F.row_number().over(window))

    invalid = df_ranked.filter(null_filter | (F.col("_row_num") > 1)).drop("_row_num")
    valid   = df_ranked.filter(~null_filter & (F.col("_row_num") == 1)).drop("_row_num")

    valid_count   = valid.count()
    invalid_count = invalid.count()
    logger.info("Valid rows: %d | Quarantined: %d", valid_count, invalid_count)

    if invalid_count > 0:
        logger.warning("%d rows sent to quarantine", invalid_count)

    # ── 3. Write ──────────────────────────────────────────────────────────
    write_parquet(valid,   storage.silver_path("stg_orders"))
    write_parquet(invalid, storage.silver_path("stg_orders_quarantine"))

    logger.info("etl_orders completed.")


if __name__ == "__main__":
    run()
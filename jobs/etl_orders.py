from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.config import load_storage_config
from core.io import write_parquet
from core.logger import logger
from core.spark_session import get_spark


PRIMARY_KEY = "order_id"


def normalize_orders(df: DataFrame) -> DataFrame:
    return (
        df.select(*[F.trim(F.col(c)).alias(c) if t ==
                  "string" else F.col(c) for c, t in df.dtypes])
        .withColumn("order_purchase_ts", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_ts", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_ts", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_ts", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_ts", F.to_timestamp("order_estimated_delivery_date"))
    )


def run():
    spark = get_spark("etl_orders")
    storage = load_storage_config()

    raw_orders = spark.read.parquet(storage.bronze_path("orders"))
    cleaned = normalize_orders(raw_orders)

    valid = cleaned.filter(F.col(PRIMARY_KEY).isNotNull()
                           ).dropDuplicates([PRIMARY_KEY])
    invalid = cleaned.filter(F.col(PRIMARY_KEY).isNull()).withColumn(
        "dq_reason", F.lit("missing_order_id"))

    write_parquet(valid, storage.silver_path("stg_orders"))
    write_parquet(invalid, storage.silver_path("stg_orders_quarantine"))

    logger.info("Orders staging done. valid=%s invalid=%s",
                valid.count(), invalid.count())


if __name__ == "__main__":
    run()

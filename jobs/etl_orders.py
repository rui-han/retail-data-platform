from core.spark_session import get_spark
from core.logger import logger


def run():
    spark = get_spark("etl_orders")

    logger.info("Loading raw orders...")

    df = spark.read.csv(
        "s3a://raw/olist_orders_dataset.csv",
        header=True,
        inferSchema=True
    )

    logger.info(f"raw count = {df.count()}")

    df_clean = df.dropna()

    logger.info(f"clean count = {df_clean.count()}")

    df_clean.write.mode("overwrite").parquet(
        "s3a://clean/orders"
    )

    logger.info("ETL for orders completed successfully.")


if __name__ == "__main__":
    run()

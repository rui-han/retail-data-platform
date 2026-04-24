from pyspark.sql import functions as F

from core.config import load_storage_config
from core.io import write_parquet
from core.spark_session import get_spark


def run():
    spark = get_spark("build_order_kpis")
    storage = load_storage_config()

    fct_order_items = spark.read.parquet(storage.silver_path("fct_order_items"))

    daily_kpis = (
        fct_order_items
        .groupBy("order_date", "order_status")
        .agg(
            F.countDistinct("order_id").alias("order_cnt"),
            F.round(F.sum("price"), 2).alias("gmv"),
            F.round(F.avg("is_delayed"), 4).alias("delay_rate"),
        )
        .orderBy("order_date", "order_status")
    )

    write_parquet(daily_kpis, storage.gold_path("mart_order_status_daily"))


if __name__ == "__main__":
    run()

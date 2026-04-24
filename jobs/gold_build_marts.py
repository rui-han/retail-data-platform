from pyspark.sql import functions as F

from core.config import load_storage_config
from core.io import write_parquet
from core.spark_session import get_spark


def run():
    spark = get_spark("gold_build_marts")
    storage = load_storage_config()

    fct_order_items = spark.read.parquet(storage.silver_path("fct_order_items"))
    dim_customers = spark.read.parquet(storage.silver_path("dim_customers"))
    dim_products = spark.read.parquet(storage.silver_path("dim_products"))

    fct_enriched = (
        fct_order_items
        .join(dim_customers.select("customer_id", "customer_state"), on="customer_id", how="left")
        .join(dim_products.select("product_id", "product_category_name_en"), on="product_id", how="left")
    )

    mart_daily_sales = (
        fct_enriched.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("price"), 2).alias("gmv"),
            F.round(F.sum("freight_value"), 2).alias("freight_total"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
            F.round(F.avg("is_delayed"), 4).alias("delay_rate"),
        )
        .orderBy("order_date")
    )

    mart_state_performance = (
        fct_enriched.groupBy("customer_state")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("price"), 2).alias("gmv"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("gmv"))
    )

    mart_category_performance = (
        fct_enriched.groupBy("product_category_name_en")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("price"), 2).alias("gmv"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("gmv"))
    )

    write_parquet(mart_daily_sales, storage.gold_path("mart_daily_sales"))
    write_parquet(mart_state_performance, storage.gold_path("mart_state_performance"))
    write_parquet(mart_category_performance, storage.gold_path("mart_category_performance"))


if __name__ == "__main__":
    run()

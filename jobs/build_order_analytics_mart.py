from pyspark.sql import functions as F

from core.config import load_storage_config
from core.io import write_parquet
from core.spark_session import get_spark


def run():
    spark = get_spark("build_order_analytics_mart")
    storage = load_storage_config()

    fct_order_items = spark.read.parquet(storage.silver_path("fct_order_items"))
    dim_customers = spark.read.parquet(storage.silver_path("dim_customers"))
    dim_products = spark.read.parquet(storage.silver_path("dim_products"))
    dim_sellers = spark.read.parquet(storage.silver_path("dim_sellers"))

    mart = (
        fct_order_items
        .join(dim_customers, on="customer_id", how="left")
        .join(dim_products.select("product_id", "product_category_name_en"), on="product_id", how="left")
        .join(dim_sellers.select("seller_id", "seller_city", "seller_state"), on="seller_id", how="left")
        .select(
            "order_id",
            "order_item_id",
            "order_date",
            "order_status",
            "customer_state",
            "customer_city",
            "seller_state",
            "seller_city",
            "product_id",
            "product_category_name_en",
            "price",
            "freight_value",
            "payment_value_total",
            "payment_type_primary",
            "review_score",
            F.current_timestamp().alias("etl_loaded_at"),
        )
    )

    write_parquet(mart, storage.gold_path("mart_order_analytics"))


if __name__ == "__main__":
    run()

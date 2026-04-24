from pyspark.sql import functions as F

from core.config import load_storage_config
from core.io import write_parquet
from core.spark_session import get_spark


def run():
    spark = get_spark("silver_build_model")
    storage = load_storage_config()

    customers = spark.read.parquet(storage.bronze_path("customers"))
    sellers = spark.read.parquet(storage.bronze_path("sellers"))
    products = spark.read.parquet(storage.bronze_path("products"))
    category_translation = spark.read.parquet(storage.bronze_path("category_translation"))
    geolocation = spark.read.parquet(storage.bronze_path("geolocation"))

    orders = spark.read.parquet(storage.bronze_path("orders"))
    order_items = spark.read.parquet(storage.bronze_path("order_items"))
    order_payments = spark.read.parquet(storage.bronze_path("order_payments"))
    order_reviews = spark.read.parquet(storage.bronze_path("order_reviews"))

    dim_customers = (
        customers
        .dropDuplicates(["customer_id"])
        .select(
            "customer_id",
            "customer_unique_id",
            F.lower(F.trim(F.col("customer_city"))).alias("customer_city"),
            F.col("customer_state"),
            "customer_zip_code_prefix",
        )
    )

    dim_sellers = (
        sellers
        .dropDuplicates(["seller_id"])
        .select(
            "seller_id",
            F.lower(F.trim(F.col("seller_city"))).alias("seller_city"),
            "seller_state",
            "seller_zip_code_prefix",
        )
    )

    geolocation_clean = (
        geolocation
        .withColumn("geolocation_city", F.lower(F.trim(F.col("geolocation_city"))))
        .groupBy("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state")
        .agg(
            F.avg("geolocation_lat").alias("avg_lat"),
            F.avg("geolocation_lng").alias("avg_lng"),
        )
    )

    dim_products = (
        products
        .join(category_translation, on="product_category_name", how="left")
        .withColumn(
            "product_category_name_en",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
        )
        .drop("product_category_name_english")
    )

    orders_clean = (
        orders
        .dropDuplicates(["order_id"])
        .withColumn("order_purchase_ts", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_ts", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_ts", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_ts", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_ts", F.to_timestamp("order_estimated_delivery_date"))
    )

    payments_agg = order_payments.groupBy("order_id").agg(
        F.sum(F.col("payment_value").cast("double")).alias("payment_value_total"),
        F.first("payment_type", ignorenulls=True).alias("payment_type_primary"),
        F.sum(F.col("payment_installments").cast("int")).alias("payment_installments_total"),
    )

    reviews_agg = order_reviews.groupBy("order_id").agg(
        F.max(F.col("review_score").cast("int")).alias("review_score"),
        F.max(F.to_timestamp("review_creation_date")).alias("review_creation_ts"),
    )

    fct_order_items = (
        order_items
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .join(orders_clean.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_ts",
            "order_delivered_customer_ts",
            "order_estimated_delivery_ts",
        ), on="order_id", how="left")
        .join(payments_agg, on="order_id", how="left")
        .join(reviews_agg, on="order_id", how="left")
        .withColumn("order_date", F.to_date("order_purchase_ts"))
        .withColumn("is_delayed", F.when(F.col("order_delivered_customer_ts") > F.col("order_estimated_delivery_ts"), F.lit(1)).otherwise(F.lit(0)))
        .dropDuplicates(["order_id", "order_item_id"])
    )

    write_parquet(dim_customers, storage.silver_path("dim_customers"))
    write_parquet(dim_sellers, storage.silver_path("dim_sellers"))
    write_parquet(dim_products, storage.silver_path("dim_products"))
    write_parquet(geolocation_clean, storage.silver_path("dim_geolocation"))
    write_parquet(fct_order_items, storage.silver_path("fct_order_items"))


if __name__ == "__main__":
    run()

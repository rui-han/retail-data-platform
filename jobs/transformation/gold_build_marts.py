"""
gold_build_marts.py — Aggregated business marts + wide analytics table.

Reads from silver layer once, builds a shared enriched DataFrame,
then derives all four gold marts from it to avoid redundant I/O and joins.

Writes to:
  gold/mart_order_analytics      — wide table for BI / ad-hoc exploration
  gold/mart_daily_sales          — day-level GMV and KPIs
  gold/mart_state_performance    — state-level GMV and KPIs
  gold/mart_category_performance — category-level GMV and KPIs
  gold/mart_order_status_daily   — daily KPIs broken out by order status
"""

from pyspark.sql import functions as F

from core.config import load_storage_config
from core.io import write_parquet
from core.logger import logger
from core.spark_session import get_spark


def run():
    spark = get_spark("gold_build_marts")
    storage = load_storage_config()

    # ── Read silver once ──────────────────────────────────────────────────
    fct   = spark.read.parquet(storage.silver_path("fct_order_items"))
    dim_c = spark.read.parquet(storage.silver_path("dim_customers"))
    dim_p = spark.read.parquet(storage.silver_path("dim_products"))
    dim_s = spark.read.parquet(storage.silver_path("dim_sellers"))

    # ── One shared enriched table — cache so all marts read from memory ──
    fct_enriched = (
        fct
        .join(dim_c.select("customer_id", "customer_state", "customer_city"),
              on="customer_id", how="left")
        .join(dim_p.select("product_id", "product_category_name_en"),
              on="product_id",   how="left")
        .join(dim_s.select("seller_id", "seller_city", "seller_state"),
              on="seller_id",    how="left")
        .cache()
    )

    enriched_count = fct_enriched.count()   # triggers the cache
    logger.info("Enriched fact rows cached: %d", enriched_count)

    # ── mart_order_analytics (wide, row-level) ───────────────────────────
    mart_order_analytics = fct_enriched.select(
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
        "is_delayed",
        F.current_timestamp().alias("etl_loaded_at"),
    )

    # ── mart_daily_sales ──────────────────────────────────────────────────
    # delay_rate: F.avg ignores null rows (undelivered orders), so the rate
    # is computed only over orders with a known delivery outcome. This is
    # intentional — undelivered orders should not be treated as on-time.
    mart_daily_sales = (
        fct_enriched.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("price"),         2).alias("gmv"),
            F.round(F.sum("freight_value"), 2).alias("freight_total"),
            F.round(F.avg("review_score"),  2).alias("avg_review_score"),
            F.round(F.avg("is_delayed"),    4).alias("delay_rate"),
        )
        .orderBy("order_date")
    )

    # ── mart_state_performance ────────────────────────────────────────────
    mart_state_performance = (
        fct_enriched.groupBy("customer_state")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("price"),        2).alias("gmv"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("gmv"))
    )

    # ── mart_category_performance ─────────────────────────────────────────
    mart_category_performance = (
        fct_enriched.groupBy("product_category_name_en")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("price"),        2).alias("gmv"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("gmv"))
    )

    # ── mart_order_status_daily ───────────────────────────────────────────
    mart_order_status_daily = (
        fct_enriched.groupBy("order_date", "order_status")
        .agg(
            F.countDistinct("order_id").alias("order_cnt"),
            F.round(F.sum("price"),      2).alias("gmv"),
            F.round(F.avg("is_delayed"), 4).alias("delay_rate"),
        )
        .orderBy("order_date", "order_status")
    )

    # ── Write ─────────────────────────────────────────────────────────────
    write_parquet(mart_order_analytics,     storage.gold_path("mart_order_analytics"))
    write_parquet(mart_daily_sales,         storage.gold_path("mart_daily_sales"))
    write_parquet(mart_state_performance,   storage.gold_path("mart_state_performance"))
    write_parquet(mart_category_performance,storage.gold_path("mart_category_performance"))
    write_parquet(mart_order_status_daily,  storage.gold_path("mart_order_status_daily"))

    fct_enriched.unpersist()
    logger.info("Gold marts build complete.")


if __name__ == "__main__":
    run()
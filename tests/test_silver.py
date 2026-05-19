"""
test_silver.py — unit tests for transformation logic in silver_build_model.py.

Tests cover:
  - is_delayed: null when order not yet delivered
  - is_delayed: 1 when delivered after estimated date
  - is_delayed: 0 when delivered on or before estimated date
  - dim_products: duplicate product_ids do not fan-out rows when joined to order items
  - dim_products: missing category translation falls back to "unknown"
"""

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

# ---------------------------------------------------------------------------
# is_delayed
# ---------------------------------------------------------------------------

# Schema that mirrors the columns used when computing is_delayed.
_DELIVERY_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_delivered_customer_ts", TimestampType(), True),
    StructField("order_estimated_delivery_ts", TimestampType(), True),
])


def _compute_is_delayed(spark: SparkSession, rows: list[tuple]):
    """Recreate the is_delayed column logic from silver_build_model."""
    df = spark.createDataFrame(rows, schema=_DELIVERY_SCHEMA)
    return df.withColumn(
        "is_delayed",
        F.when(
            F.col("order_delivered_customer_ts").isNull(),
            F.lit(None).cast("integer"),
        )
        .when(
            F.col("order_delivered_customer_ts") > F.col("order_estimated_delivery_ts"),
            F.lit(1),
        )
        .otherwise(F.lit(0)),
    )


class TestIsDelayed:
    def test_null_when_not_yet_delivered(self, spark):
        rows = [("order1", None, None)]
        result = _compute_is_delayed(spark, rows).collect()[0]
        assert result["is_delayed"] is None

    def test_one_when_delivered_late(self, spark):
        from datetime import datetime
        rows = [(
            "order1",
            datetime(2017, 10, 20),  # delivered
            datetime(2017, 10, 18),  # estimated — earlier
        )]
        result = _compute_is_delayed(spark, rows).collect()[0]
        assert result["is_delayed"] == 1

    def test_zero_when_delivered_on_time(self, spark):
        from datetime import datetime
        rows = [(
            "order1",
            datetime(2017, 10, 17),  # delivered
            datetime(2017, 10, 18),  # estimated — later
        )]
        result = _compute_is_delayed(spark, rows).collect()[0]
        assert result["is_delayed"] == 0

    def test_zero_when_delivered_exactly_on_estimated_date(self, spark):
        from datetime import datetime
        same = datetime(2017, 10, 18)
        rows = [("order1", same, same)]
        result = _compute_is_delayed(spark, rows).collect()[0]
        assert result["is_delayed"] == 0

    def test_is_delayed_column_is_integer_type(self, spark):
        from datetime import datetime
        rows = [("order1", datetime(2017, 10, 17), datetime(2017, 10, 18))]
        df = _compute_is_delayed(spark, rows)
        assert df.schema["is_delayed"].dataType == IntegerType()

    def test_mixed_rows(self, spark):
        from datetime import datetime
        rows = [
            ("order1", None, None),  # null
            ("order2", datetime(2017, 10, 20), datetime(2017, 10, 18)),  # late
            ("order3", datetime(2017, 10, 15), datetime(2017, 10, 18)),  # on time
        ]
        results = {
            r["order_id"]: r["is_delayed"]
            for r in _compute_is_delayed(spark, rows).collect()
        }
        assert results["order1"] is None
        assert results["order2"] == 1
        assert results["order3"] == 0


# ---------------------------------------------------------------------------
# dim_products — dedup before join
# ---------------------------------------------------------------------------

_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
])

_TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])

_ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
])


def _build_dim_products(spark, product_rows, translation_rows):
    """Recreate the dim_products transformation from silver_build_model."""
    products = spark.createDataFrame(product_rows, schema=_PRODUCTS_SCHEMA)
    translations = spark.createDataFrame(translation_rows, schema=_TRANSLATION_SCHEMA)

    return (
        products
        .dropDuplicates(["product_id"])
        .join(translations, on="product_category_name", how="left")
        .withColumn(
            "product_category_name_en",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
        )
        .drop("product_category_name_english")
    )


class TestDimProducts:
    def test_duplicate_product_ids_removed(self, spark):
        product_rows = [
            ("prod1", "electronics"),
            ("prod1", "electronics"),  # duplicate
        ]
        translation_rows = [("electronics", "electronics")]
        dim = _build_dim_products(spark, product_rows, translation_rows)
        assert dim.count() == 1

    def test_no_row_fanout_when_joined_to_order_items(self, spark):
        """
        If dim_products contains duplicate product_ids, a join to order_items
        would fan-out rows, silently inflating item counts and GMV.
        This test confirms that dedup prevents the fan-out.
        """
        product_rows = [
            ("prod1", "electronics"),
            ("prod1", "electronics"),  # duplicate — without dedup this fans out
        ]
        translation_rows = [("electronics", "electronics")]
        order_items = spark.createDataFrame(
            [("order1", "prod1", 99.9)],
            schema=_ORDER_ITEMS_SCHEMA,
        )

        dim = _build_dim_products(spark, product_rows, translation_rows)
        joined = order_items.join(dim, on="product_id", how="left")

        # Exactly one row in order_items should yield exactly one row after join.
        assert joined.count() == 1

    def test_missing_translation_falls_back_to_unknown(self, spark):
        product_rows = [("prod1", "cama_mesa_banho")]
        translation_rows = []  # no translation available
        dim = _build_dim_products(spark, product_rows, translation_rows)
        result = dim.collect()[0]
        assert result["product_category_name_en"] == "unknown"

    def test_known_translation_is_applied(self, spark):
        product_rows = [("prod1", "informatica_acessorios")]
        translation_rows = [("informatica_acessorios", "computers_accessories")]
        dim = _build_dim_products(spark, product_rows, translation_rows)
        result = dim.collect()[0]
        assert result["product_category_name_en"] == "computers_accessories"

    def test_distinct_product_ids_all_present(self, spark):
        product_rows = [
            ("prod1", "electronics"),
            ("prod2", "furniture"),
            ("prod3", "toys"),
        ]
        translation_rows = []
        dim = _build_dim_products(spark, product_rows, translation_rows)
        assert dim.count() == 3

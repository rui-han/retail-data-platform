"""
test_etl_orders.py — unit tests for the DQ gate logic in jobs/etl/etl_orders.py.

The job's run() function is tightly coupled to storage, so we test the core
transformation logic directly by recreating it here with small in-memory
DataFrames.  If the job is later refactored to expose a transform() helper,
these tests can call that instead.

Tests cover:
  - Rows with nulls in required columns are quarantined
  - Valid rows pass through cleanly
  - Duplicate order_ids: first row by purchase timestamp is kept, rest quarantined
  - Timestamp ordering, not lexicographic string ordering, determines "first"
"""

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
]

# Minimal schema that mirrors the bronze orders table.
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
])


def _apply_dq_gate(spark: SparkSession, rows: list[tuple]):
    """
    Recreate the DQ gate from etl_orders.run() using an in-memory DataFrame.
    Returns (valid_df, invalid_df).
    """
    df = spark.createDataFrame(rows, schema=ORDERS_SCHEMA)

    null_filter = F.lit(False)
    for col in REQUIRED_COLUMNS:
        null_filter = null_filter | F.col(col).isNull()

    window = (
        Window
        .partitionBy("order_id")
        .orderBy(F.to_timestamp("order_purchase_timestamp"))
    )
    df_ranked = df.withColumn("_row_num", F.row_number().over(window))

    invalid = df_ranked.filter(null_filter | (F.col("_row_num") > 1)).drop("_row_num")
    valid = df_ranked.filter(~null_filter & (F.col("_row_num") == 1)).drop("_row_num")
    return valid, invalid


# ---------------------------------------------------------------------------
# Null / missing required fields
# ---------------------------------------------------------------------------

class TestNullFiltering:
    def test_null_order_id_is_quarantined(self, spark):
        rows = [(None, "cust1", "delivered", "2017-10-02 10:56:33")]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_null_customer_id_is_quarantined(self, spark):
        rows = [("order1", None, "delivered", "2017-10-02 10:56:33")]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_null_order_status_is_quarantined(self, spark):
        rows = [("order1", "cust1", None, "2017-10-02 10:56:33")]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_null_purchase_timestamp_is_quarantined(self, spark):
        rows = [("order1", "cust1", "delivered", None)]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_fully_valid_row_passes(self, spark):
        rows = [("order1", "cust1", "delivered", "2017-10-02 10:56:33")]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 1
        assert invalid.count() == 0


# ---------------------------------------------------------------------------
# Duplicate order_id deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_duplicate_order_id_keeps_one(self, spark):
        rows = [
            ("order1", "cust1", "delivered", "2017-10-02 10:56:33"),
            ("order1", "cust1", "delivered", "2017-10-03 08:00:00"),  # duplicate
        ]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 1
        assert invalid.count() == 1

    def test_duplicate_order_id_keeps_earliest_timestamp(self, spark):
        rows = [
            ("order1", "cust1", "delivered", "2017-10-03 08:00:00"),  # later
            ("order1", "cust1", "delivered", "2017-10-02 10:56:33"),  # earlier — should be kept
        ]
        valid, _ = _apply_dq_gate(spark, rows)
        kept = valid.collect()[0]
        assert kept["order_purchase_timestamp"] == "2017-10-02 10:56:33"

    def test_timestamp_ordering_not_lexicographic(self, spark):
        """
        '2017-09-30' < '2017-10-02' chronologically but
        '2017-09-30' > '2017-10-02' lexicographically (digit '9' > '1').
        The correct row to keep is '2017-09-30'.
        """
        rows = [
            ("order1", "cust1", "delivered", "2017-10-02 10:56:33"),
            ("order1", "cust1", "delivered", "2017-09-30 08:00:00"),  # earlier, but lex-greater
        ]
        valid, _ = _apply_dq_gate(spark, rows)
        kept = valid.collect()[0]
        assert kept["order_purchase_timestamp"] == "2017-09-30 08:00:00"

    def test_three_duplicates_keeps_only_first(self, spark):
        rows = [
            ("order1", "cust1", "delivered", "2017-10-04 00:00:00"),
            ("order1", "cust1", "delivered", "2017-10-02 00:00:00"),  # earliest
            ("order1", "cust1", "delivered", "2017-10-03 00:00:00"),
        ]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 1
        assert invalid.count() == 2

    def test_distinct_order_ids_all_pass(self, spark):
        rows = [
            ("order1", "cust1", "delivered", "2017-10-02 10:56:33"),
            ("order2", "cust2", "shipped", "2017-10-03 09:00:00"),
            ("order3", "cust3", "approved", "2017-10-04 11:00:00"),
        ]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 3
        assert invalid.count() == 0


# ---------------------------------------------------------------------------
# Mixed cases
# ---------------------------------------------------------------------------

class TestMixedCases:
    def test_null_row_in_duplicate_group_is_quarantined(self, spark):
        rows = [
            ("order1", "cust1", "delivered", "2017-10-02 10:56:33"),
            ("order1", None, "delivered", "2017-10-03 08:00:00"),  # null + duplicate
        ]
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 1
        assert invalid.count() == 1

    def test_empty_dataframe(self, spark):
        rows = []
        valid, invalid = _apply_dq_gate(spark, rows)
        assert valid.count() == 0
        assert invalid.count() == 0

"""
test_io.py — unit tests for core/io.py.

Tests cover:
  - _build_schema returns correct field names and types for known tables
  - _build_schema returns None for unknown tables (inferSchema fallback)
  - read_raw_csv applies explicit schema when table is known
  - read_raw_csv falls back to inferSchema when table is unknown
  - write_parquet round-trips a DataFrame correctly
"""

import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from core.io import _build_schema, read_raw_csv, write_parquet


# ---------------------------------------------------------------------------
# _build_schema
# ---------------------------------------------------------------------------

class TestBuildSchema:
    def test_known_table_returns_struct_type(self):
        schema = _build_schema("orders")
        assert isinstance(schema, StructType)

    def test_unknown_table_returns_none(self):
        assert _build_schema("nonexistent_table") is None

    def test_orders_schema_has_order_id_as_string(self):
        schema = _build_schema("orders")
        field = next(f for f in schema.fields if f.name == "order_id")
        assert isinstance(field.dataType, StringType)

    def test_order_items_price_is_double(self):
        schema = _build_schema("order_items")
        field = next(f for f in schema.fields if f.name == "price")
        assert isinstance(field.dataType, DoubleType)

    def test_order_items_order_item_id_is_integer(self):
        schema = _build_schema("order_items")
        field = next(f for f in schema.fields if f.name == "order_item_id")
        assert isinstance(field.dataType, IntegerType)

    def test_all_fields_nullable(self):
        # Every field in every schema should be nullable=True so that
        # missing values in raw CSVs don't hard-fail on read.
        for table in ("orders", "order_items", "order_payments", "customers"):
            schema = _build_schema(table)
            for field in schema.fields:
                assert field.nullable, (
                    f"{table}.{field.name} should be nullable"
                )


# ---------------------------------------------------------------------------
# read_raw_csv
# ---------------------------------------------------------------------------

class TestReadRawCsv:
    """Uses a temporary CSV written to the local filesystem."""

    @pytest.fixture()
    def orders_csv(self, tmp_path) -> str:
        content = (
            "order_id,customer_id,order_status,order_purchase_timestamp,"
            "order_approved_at,order_delivered_carrier_date,"
            "order_delivered_customer_date,order_estimated_delivery_date\n"
            "abc123,cust1,delivered,2017-10-02 10:56:33,"
            "2017-10-02 11:07:15,2017-10-04 19:55:00,"
            "2017-10-10 21:25:13,2017-10-18 00:00:00\n"
        )
        path = tmp_path / "orders.csv"
        path.write_text(content)
        return str(path)

    def test_explicit_schema_applied(self, spark, orders_csv):
        df = read_raw_csv(spark, orders_csv, table="orders")
        assert df.schema["order_id"].dataType == StringType()

    def test_row_count(self, spark, orders_csv):
        df = read_raw_csv(spark, orders_csv, table="orders")
        assert df.count() == 1

    def test_fallback_to_infer_schema_for_unknown_table(self, spark, orders_csv):
        # Should not raise — just uses inferSchema.
        df = read_raw_csv(spark, orders_csv, table="unknown_table")
        assert df.count() == 1

    def test_empty_table_arg_uses_infer_schema(self, spark, orders_csv):
        df = read_raw_csv(spark, orders_csv)
        assert df.count() == 1


# ---------------------------------------------------------------------------
# write_parquet
# ---------------------------------------------------------------------------

class TestWriteParquet:
    def test_round_trip(self, spark, tmp_path):
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True),
        ])
        original = spark.createDataFrame([("a", 1), ("b", 2)], schema=schema)
        path = str(tmp_path / "out")

        write_parquet(original, path)
        reloaded = spark.read.parquet(path)

        assertDataFrameEqual(original, reloaded)

    def test_overwrite_replaces_existing(self, spark, tmp_path):
        schema = StructType([StructField("id", StringType(), True)])
        path = str(tmp_path / "out")

        write_parquet(spark.createDataFrame([("old",)], schema=schema), path)
        write_parquet(spark.createDataFrame([("new",)], schema=schema), path)

        result = spark.read.parquet(path).collect()
        assert len(result) == 1
        assert result[0]["id"] == "new"

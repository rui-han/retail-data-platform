from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)

from core.config import OLIST_SCHEMAS
from core.logger import logger


# Map simple type-name strings to PySpark types so schemas stay
# readable in config.py without importing PySpark there.
_TYPE_MAP = {
    "string":  StringType(),
    "integer": IntegerType(),
    "double":  DoubleType(),
}


def _build_schema(table: str) -> StructType | None:
    """Return a StructType for *table* if a definition exists, else None."""
    fields = OLIST_SCHEMAS.get(table)
    if not fields:
        return None
    return StructType([
        StructField(name, _TYPE_MAP[dtype], nullable=True)
        for name, dtype in fields
    ])


# FIX: path is now a caller-supplied parameter instead of being resolved
# internally via load_storage_config(). This removes the hidden per-call
# overhead of reading environment variables on every table in the bronze loop,
# and makes the function straightforward to unit-test without env var mocking.
def read_raw_csv(spark: SparkSession, path: str, table: str = "") -> DataFrame:
    """
    Read a raw CSV from *path*.

    If *table* matches a key in OLIST_SCHEMAS the schema is applied
    explicitly, avoiding a full-file scan and type-inference surprises.
    When no schema is found, inferSchema falls back gracefully.
    """
    schema = _build_schema(table)
    if schema:
        logger.info("Reading %s with explicit schema (%d fields)",
                    path, len(schema))
        df = spark.read.csv(path, header=True, schema=schema)
    else:
        logger.warning(
            "No explicit schema for table '%s', falling back to inferSchema", table)
        df = spark.read.csv(path, header=True, inferSchema=True)

    return df


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write *df* to *path* as Parquet. Logs after the write completes."""
    df.write.mode(mode).parquet(path)
    logger.info("Written → %s (mode=%s)", path, mode)
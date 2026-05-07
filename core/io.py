from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)

from core.config import OLIST_SCHEMAS, load_storage_config
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


def read_raw_csv(spark: SparkSession, filename: str, table: str = "") -> DataFrame:
    """
    Read a raw CSV from the configured raw bucket.

    If *table* matches a key in OLIST_SCHEMAS the schema is applied
    explicitly, avoiding a full-file scan and type-inference surprises.
    When no schema is found, inferSchema falls back gracefully.
    """
    storage = load_storage_config()
    path = storage.raw_path(filename)

    schema = _build_schema(table or filename.replace(".csv", ""))
    if schema:
        logger.info("Reading %s with explicit schema (%d fields)",
                    path, len(schema))
        df = spark.read.csv(path, header=True, schema=schema)
    else:
        logger.warning(
            "No explicit schema for '%s', falling back to inferSchema", filename)
        df = spark.read.csv(path, header=True, inferSchema=True)

    logger.info("Loaded %s → %d rows", path, df.count())
    return df


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    count = df.count()
    logger.info("Writing %d rows → %s (mode=%s)", count, path, mode)
    df.write.mode(mode).parquet(path)

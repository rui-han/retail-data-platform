"""
conftest.py — shared fixtures for the test suite.

The SparkSession is created once per test session (scope="session") to avoid
the ~10 s JVM startup cost on every test module.  Individual tests must not
call spark.stop() — pytest handles teardown via the session-scoped fixture.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Minimal local SparkSession for unit tests — no S3 / MinIO required."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("retail-platform-tests")
        # Reduce shuffle partitions so small test DataFrames don't spin up
        # 200 empty tasks.
        .config("spark.sql.shuffle.partitions", "1")
        # Suppress most Spark INFO noise in test output.
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=WARN")
        .getOrCreate()
    )
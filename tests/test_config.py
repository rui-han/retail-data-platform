"""
test_config.py — unit tests for core/config.py.

Tests cover:
  - StorageConfig path construction (raw, bronze, silver, gold)
  - Trailing-slash normalisation in base paths
  - _require_env raises EnvironmentError when the variable is absent
  - _require_env returns the value when the variable is set
"""

import os
import pytest

from core.config import StorageConfig, _require_env


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def storage() -> StorageConfig:
    """A StorageConfig with predictable base paths for assertion."""
    return StorageConfig(
        endpoint="http://localhost:9000",
        access_key="test-key",
        secret_key="test-secret",
        raw_base_path="s3a://raw",
        clean_base_path="s3a://clean/olist",
        mart_base_path="s3a://mart/olist",
    )


@pytest.fixture()
def storage_trailing_slash() -> StorageConfig:
    """Base paths with trailing slashes — should be normalised by path methods."""
    return StorageConfig(
        endpoint="http://localhost:9000",
        access_key="test-key",
        secret_key="test-secret",
        raw_base_path="s3a://raw/",
        clean_base_path="s3a://clean/olist/",
        mart_base_path="s3a://mart/olist/",
    )


# ---------------------------------------------------------------------------
# StorageConfig.raw_path
# ---------------------------------------------------------------------------

class TestRawPath:
    def test_basic(self, storage):
        assert storage.raw_path("orders.csv") == "s3a://raw/olist/latest/orders.csv"

    def test_trailing_slash_normalised(self, storage_trailing_slash):
        result = storage_trailing_slash.raw_path("orders.csv")
        assert "//" not in result.replace("s3a://", "")


# ---------------------------------------------------------------------------
# StorageConfig.bronze_path / silver_path / gold_path
# ---------------------------------------------------------------------------

class TestLayerPaths:
    def test_bronze_path(self, storage):
        assert storage.bronze_path("orders") == "s3a://clean/olist/bronze/orders"

    def test_silver_path(self, storage):
        assert storage.silver_path("stg_orders") == "s3a://clean/olist/silver/stg_orders"

    def test_gold_path(self, storage):
        assert storage.gold_path("mart_daily_sales") == "s3a://mart/olist/gold/mart_daily_sales"

    def test_bronze_trailing_slash_normalised(self, storage_trailing_slash):
        result = storage_trailing_slash.bronze_path("orders")
        assert "//" not in result.replace("s3a://", "")

    def test_silver_trailing_slash_normalised(self, storage_trailing_slash):
        result = storage_trailing_slash.silver_path("stg_orders")
        assert "//" not in result.replace("s3a://", "")

    def test_gold_trailing_slash_normalised(self, storage_trailing_slash):
        result = storage_trailing_slash.gold_path("mart_daily_sales")
        assert "//" not in result.replace("s3a://", "")


# ---------------------------------------------------------------------------
# _require_env
# ---------------------------------------------------------------------------

class TestRequireEnv:
    def test_raises_when_missing(self):
        var = "_TEST_VAR_THAT_DOES_NOT_EXIST_"
        os.environ.pop(var, None)
        with pytest.raises(EnvironmentError, match=var):
            _require_env(var)

    def test_raises_when_empty(self):
        var = "_TEST_VAR_EMPTY_"
        os.environ[var] = ""
        try:
            with pytest.raises(EnvironmentError):
                _require_env(var)
        finally:
            os.environ.pop(var, None)

    def test_raises_when_whitespace_only(self):
        var = "_TEST_VAR_WHITESPACE_"
        os.environ[var] = "   "
        try:
            with pytest.raises(EnvironmentError):
                _require_env(var)
        finally:
            os.environ.pop(var, None)

    def test_returns_value_when_set(self):
        var = "_TEST_VAR_SET_"
        os.environ[var] = "hello"
        try:
            assert _require_env(var) == "hello"
        finally:
            os.environ.pop(var, None)

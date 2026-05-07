from __future__ import annotations

import os
from dataclasses import dataclass


OLIST_DATASETS = {
    "customers":          "olist_customers_dataset.csv",
    "geolocation":        "olist_geolocation_dataset.csv",
    "order_items":        "olist_order_items_dataset.csv",
    "order_payments":     "olist_order_payments_dataset.csv",
    "order_reviews":      "olist_order_reviews_dataset.csv",
    "orders":             "olist_orders_dataset.csv",
    "products":           "olist_products_dataset.csv",
    "sellers":            "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# Explicit schemas prevent inferSchema surprises (e.g. order_id cast to int).
# Import inside jobs to avoid a PySpark dependency in config itself.
OLIST_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "customers": [
        ("customer_id",              "string"),
        ("customer_unique_id",       "string"),
        ("customer_zip_code_prefix", "string"),
        ("customer_city",            "string"),
        ("customer_state",           "string"),
    ],
    "geolocation": [
        ("geolocation_zip_code_prefix", "string"),
        ("geolocation_lat",             "double"),
        ("geolocation_lng",             "double"),
        ("geolocation_city",            "string"),
        ("geolocation_state",           "string"),
    ],
    "order_items": [
        ("order_id",             "string"),
        ("order_item_id",        "integer"),
        ("product_id",           "string"),
        ("seller_id",            "string"),
        ("shipping_limit_date",  "string"),
        ("price",                "double"),
        ("freight_value",        "double"),
    ],
    "order_payments": [
        ("order_id",               "string"),
        ("payment_sequential",     "integer"),
        ("payment_type",           "string"),
        ("payment_installments",   "integer"),
        ("payment_value",          "double"),
    ],
    "order_reviews": [
        ("review_id",               "string"),
        ("order_id",                "string"),
        ("review_score",            "integer"),
        ("review_comment_title",    "string"),
        ("review_comment_message",  "string"),
        ("review_creation_date",    "string"),
        ("review_answer_timestamp", "string"),
    ],
    "orders": [
        ("order_id",                        "string"),
        ("customer_id",                     "string"),
        ("order_status",                    "string"),
        ("order_purchase_timestamp",        "string"),
        ("order_approved_at",               "string"),
        ("order_delivered_carrier_date",    "string"),
        ("order_delivered_customer_date",   "string"),
        ("order_estimated_delivery_date",   "string"),
    ],
    "products": [
        ("product_id",                 "string"),
        ("product_category_name",      "string"),
        ("product_name_length",        "integer"),
        ("product_description_length", "integer"),
        ("product_photos_qty",         "integer"),
        ("product_weight_g",           "double"),
        ("product_length_cm",          "double"),
        ("product_height_cm",          "double"),
        ("product_width_cm",           "double"),
    ],
    "sellers": [
        ("seller_id",              "string"),
        ("seller_zip_code_prefix", "string"),
        ("seller_city",            "string"),
        ("seller_state",           "string"),
    ],
    "category_translation": [
        ("product_category_name",         "string"),
        ("product_category_name_english", "string"),
    ],
}


def _require_env(name: str) -> str:
    """Return env var value or raise – never falls back to a hardcoded secret."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            "Make sure your .env file is loaded before running this job."
        )
    return value


@dataclass(frozen=True)
class StorageConfig:
    endpoint: str
    access_key: str
    secret_key: str
    raw_base_path: str
    clean_base_path: str
    mart_base_path: str

    def raw_path(self, filename: str) -> str:
        return f"{self.raw_base_path.rstrip('/')}/olist/latest/{filename}"

    def bronze_path(self, table: str) -> str:
        return f"{self.clean_base_path.rstrip('/')}/bronze/{table}"

    def silver_path(self, table: str) -> str:
        return f"{self.clean_base_path.rstrip('/')}/silver/{table}"

    def gold_path(self, table: str) -> str:
        return f"{self.mart_base_path.rstrip('/')}/gold/{table}"


def load_storage_config() -> StorageConfig:
    return StorageConfig(
        endpoint=_require_env("S3_ENDPOINT"),
        access_key=_require_env("S3_ACCESS_KEY"),
        secret_key=_require_env("S3_SECRET_KEY"),
        raw_base_path=os.getenv("RAW_BASE_PATH",  "s3a://raw"),
        clean_base_path=os.getenv("CLEAN_BASE_PATH", "s3a://clean/olist"),
        mart_base_path=os.getenv("MART_BASE_PATH",  "s3a://mart/olist"),
    )

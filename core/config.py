import os
from dataclasses import dataclass


OLIST_DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


@dataclass(frozen=True)
class StorageConfig:
    endpoint: str
    access_key: str
    secret_key: str
    raw_base_path: str
    clean_base_path: str
    mart_base_path: str

    def raw_path(self, filename: str) -> str:
        return f"{self.raw_base_path.rstrip('/')}/{filename}"

    def bronze_path(self, table: str) -> str:
        return f"{self.clean_base_path.rstrip('/')}/bronze/{table}"

    def silver_path(self, table: str) -> str:
        return f"{self.clean_base_path.rstrip('/')}/silver/{table}"

    def gold_path(self, table: str) -> str:
        return f"{self.mart_base_path.rstrip('/')}/gold/{table}"


def load_storage_config() -> StorageConfig:
    return StorageConfig(
        endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        access_key=os.getenv("S3_ACCESS_KEY", "admin"),
        secret_key=os.getenv("S3_SECRET_KEY", "password123"),
        raw_base_path=os.getenv("RAW_BASE_PATH", "s3a://raw"),
        clean_base_path=os.getenv("CLEAN_BASE_PATH", "s3a://clean/olist"),
        mart_base_path=os.getenv("MART_BASE_PATH", "s3a://mart/olist"),
    )

from pyspark.sql import SparkSession

from core.config import load_storage_config


def get_spark(app_name: str):
    storage = load_storage_config()
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", storage.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", storage.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", storage.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

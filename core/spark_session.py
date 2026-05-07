from core.config import load_storage_config
from pyspark.sql import SparkSession
import os


def get_spark(app_name: str) -> SparkSession:
    storage = load_storage_config()
    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint",            storage.endpoint)
        .config("spark.hadoop.fs.s3a.access.key",          storage.access_key)
        .config("spark.hadoop.fs.s3a.secret.key",          storage.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .getOrCreate()
    )

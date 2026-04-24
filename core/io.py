from pyspark.sql import DataFrame

from core.config import load_storage_config
from core.logger import logger


def read_raw_csv(spark, filename: str) -> DataFrame:
    storage = load_storage_config()
    path = storage.raw_path(filename)
    logger.info("Reading raw CSV %s", path)
    return spark.read.csv(path, header=True, inferSchema=True)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    logger.info("Writing parquet to %s", path)
    df.write.mode(mode).parquet(path)

from core.config import OLIST_DATASETS, load_storage_config
from core.io import read_raw_csv, write_parquet
from core.logger import logger
from core.spark_session import get_spark


def run():
    spark = get_spark("bronze_ingest_olist")
    storage = load_storage_config()

    for table, filename in OLIST_DATASETS.items():
        path = storage.raw_path(filename)
        logger.info("Ingesting table '%s' from %s", table, path)
        # FIX: pass the resolved path directly — read_raw_csv no longer calls
        # load_storage_config() internally, so storage is resolved once per job
        # rather than once per table.
        df = read_raw_csv(spark, path, table=table)
        write_parquet(df, storage.bronze_path(table))

    logger.info("Bronze ingestion complete.")


if __name__ == "__main__":
    run()
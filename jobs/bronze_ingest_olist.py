from core.config import OLIST_DATASETS, load_storage_config
from core.io import read_raw_csv, write_parquet
from core.spark_session import get_spark


def run():
    spark = get_spark("bronze_ingest_olist")
    storage = load_storage_config()

    for table, filename in OLIST_DATASETS.items():
        df = read_raw_csv(spark, filename)
        write_parquet(df, storage.bronze_path(table))


if __name__ == "__main__":
    run()

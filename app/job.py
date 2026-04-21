from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Simple Test") \
    .getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "admin")
hadoop_conf.set("fs.s3a.secret.key", "password123")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.csv(
    "s3a://raw/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

df.printSchema()

df.show(5, truncate=False)

print("Read and show success!")

spark.stop()

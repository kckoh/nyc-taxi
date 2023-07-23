from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from pyspark.sql.functions import sum

# Create a SparkSession
# no need to configure aws secret / public because it is running on EMR
spark = SparkSession.builder \
    .appName("nyc-taxi") \
    .getOrCreate()

print(spark.conf.get("spark.executorEnv.S3PATH"))

df = spark.read.parquet("s3a://nyc-taxi-project/data/ingest/yellow_tripdata_2022-10.parquet")
df_total_passenger = df.agg(sum("passenger_count"))
df_total_passenger.show()

# df.write.parquet("s3a://nyc-taxi-project/data/processed/")
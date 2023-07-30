from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
import pyspark

from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour

# Create a SparkSession
spark = SparkSession.builder \
    .appName("nyc-taxi") \
    .getOrCreate()

s3_path_list = str(spark.conf.get("spark.executorEnv.S3PATH")).split("/")
bucket, file_name, s3_path = s3_path_list[0], s3_path_list[3], str(spark.conf.get("spark.executorEnv.S3PATH"))

# reading the file
df = spark.read.parquet(f"s3a://{s3_path}")


# extracting time
df = df.withColumn("pickup_year", year("tpep_pickup_datetime"))
df = df.withColumn("pickup_month", month("tpep_pickup_datetime"))
df = df.withColumn("pickup_day", dayofmonth("tpep_pickup_datetime"))
df = df.withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))


# calculating ride duration
# df = df.withColumn("ride_duration_seconds", (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")))
# df = df.withColumn("ride_duration_minutes", col("ride_duration_seconds") / 60)
# calculating ride duration
df = df.withColumn("ride_duration_seconds", 
                   unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))
df = df.withColumn("ride_duration_minutes", col("ride_duration_seconds") / 60)

# Calculating Fare per Minute
df = df.withColumn("fare_per_minute", col("fare_amount") / col("ride_duration_minutes"))

# Calucating Fare per distance
df = df.withColumn("fare_per_distance", col("fare_amount") / col("trip_distance"))

# Writing the file  
df.write.parquet(f"s3a://{bucket}/data/processed/{file_name}")
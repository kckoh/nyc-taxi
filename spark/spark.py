from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
import pyspark

from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, to_date, sum

# Create a SparkSession
spark = SparkSession.builder \
    .appName("nyc-taxi") \
    .enableHiveSupport() \
    .getOrCreate()

s3_path_list = str(spark.conf.get("spark.executorEnv.S3PATH")).split("/")
bucket, file_name, s3_path = s3_path_list[0], s3_path_list[3], str(spark.conf.get("spark.executorEnv.S3PATH"))


# reading the file
df = spark.read.parquet(f"s3a://{s3_path}")

# different parquet files have different data types, so we need to make them same
df = df.withColumn("passenger_count", col("passenger_count").cast("double"))

df = df.withColumn("RatecodeID", col("RatecodeID").cast("double"))

df = df.withColumn("airport_fee", col("airport_fee").cast("double"))
df = df.withColumn("congestion_surcharge", col("congestion_surcharge").cast("double"))


# extracting time
# df = df.withColumn("pickup_year", year("tpep_pickup_datetime"))
# df = df.withColumn("pickup_month", month("tpep_pickup_datetime"))
# df = df.withColumn("pickup_day", dayofmonth("tpep_pickup_datetime"))
# df = df.withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
# df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
# df = df.withColumn("date", to_date("tpep_pickup_datetime"))



# calculating ride duration
# df = df.withColumn("ride_duration_seconds", (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")))
# df = df.withColumn("ride_duration_minutes", col("ride_duration_seconds") / 60)
# calculating ride duration
# df = df.withColumn("ride_duration_seconds", 
#                    unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))
# df = df.withColumn("ride_duration_minutes", col("ride_duration_seconds") / 60)

# # Calculating Fare per Minute
# df = df.withColumn("fare_per_minute", col("fare_amount") / col("ride_duration_minutes"))

# Calculate total amount group by VendorID
total_amount_by_vendor = df.groupBy("VendorID") \
    .agg(sum("total_amount").alias("total_amount_sum")) \
    .withColumnRenamed("VendorID", "VendorID_amount")

# Calculate total trip distance group by VendorID
total_distance_by_vendor = df.groupBy("VendorID") \
    .agg(sum("Trip_distance").alias("total_distance_sum")) \
    .withColumnRenamed("VendorID", "VendorID_distance")

# Calculate total trip distance group by VendorID and PULocationID
total_distance_by_vendor_pu = df.groupBy("VendorID", "PULocationID") \
    .agg(sum("Trip_distance").alias("total_distance_sum_pu")) \
    .withColumnRenamed("VendorID", "VendorID_pu") \
    .withColumnRenamed("PULocationID", "PULocationID_pu")

# Join all three results into a single DataFrame
final_result = df \
    .join(total_amount_by_vendor, df["VendorID"] == total_amount_by_vendor["VendorID_amount"], "left") \
    .join(total_distance_by_vendor, df["VendorID"] == total_distance_by_vendor["VendorID_distance"], "left") \
    .join(total_distance_by_vendor_pu, (df["VendorID"] == total_distance_by_vendor_pu["VendorID_pu"]) & (df["PULocationID"] == total_distance_by_vendor_pu["PULocationID_pu"]), "left") \
    .drop("VendorID_amount", "VendorID_distance", "VendorID_pu", "PULocationID_pu")

# Writing the file  
final_result.write.mode("append").parquet(f"s3a://{bucket}/data/processed")
# df.write.format("parquet").partitionBy("pickup_date").mode("overwrite").save(f"s3a://{bucket}/data/processed")
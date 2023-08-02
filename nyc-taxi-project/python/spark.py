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

# # Calucating Fare per distance0
# df = df.withColumn("fare_per_distance", col("fare_amount") / col("trip_distance"))

from pyspark.sql.functions import col, lit

# Group by VendorID and Aggregate
# Perform groupBy and agg operations
vendor_summary = df.groupBy("VendorID") \
    .agg(
        sum("total_amount").alias("total_amount_sum"),
        sum("trip_distance").alias("total_distance_sum")
    )

# Join the original DataFrame with the aggregated DataFrame
df = df.join(vendor_summary, on="VendorID", how="left")
# result_vendor = df.groupBy("VendorID") \
#     .agg(
#         sum("total_amount").alias("total_amount_sum"),
#         sum("trip_distance").alias("total_distance_sum")
#     ).withColumn("key", lit(1))

# Group by VendorID, Year, Month, Day and Aggregate
# result_vendor_date = df.withColumn("year", year("tpep_pickup_datetime")) \
#     .withColumn("month", month("tpep_pickup_datetime")) \
#     .withColumn("day", dayofmonth("tpep_pickup_datetime")) \
#     .groupBy("VendorID", "year", "month", "day") \
#     .count().alias("count_vendor_date").withColumn("key", lit(1))

# Group by PULocationID, Year, Month, Day, Hour and Aggregate Count
df_with_time = df.withColumn("year", year("tpep_pickup_datetime")) \
    .withColumn("month", month("tpep_pickup_datetime")) \
    .withColumn("day", dayofmonth("tpep_pickup_datetime")) \
    .withColumn("hour", hour("tpep_pickup_datetime"))

# Performing groupBy and count operation
count_location_time = df_with_time.groupBy("PULocationID", "year", "month", "day", "hour") \
    .count().alias("count_location_time")

# Joining the original DataFrame with the count DataFrame on the common columns
result_df = df_with_time.join(
    count_location_time, 
    on=["PULocationID", "year", "month", "day", "hour"], 
    how="left"
)


# Writing the file  
result_df.write.mode("append").parquet(f"s3a://{bucket}/data/processed")
# df.write.format("parquet").partitionBy("pickup_date").mode("overwrite").save(f"s3a://{bucket}/data/processed")
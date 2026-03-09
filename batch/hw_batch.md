pip install pyspark

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
df.show()

spark.stop()


2.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hw6").getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df.repartition(4).write.parquet("yellow_output")

3.
from pyspark.sql.functions import col, to_date, lit

df.filter(
    to_date(col("tpep_pickup_datetime")) == lit("2025-11-15")
).count()


4.
from pyspark.sql.functions import col, max

df_trip = df.withColumn(
    "trip_hours",
    (col("tpep_dropoff_datetime").cast("long") -
     col("tpep_pickup_datetime").cast("long")) / 3600
)

df_trip.select(max("trip_hours")).show()

6.
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("hw6").getOrCreate()

# Load trip data
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Load zones
zones = spark.read.option("header", True).csv("taxi_zone_lookup.csv")

# Join pickup location ID to zone names
df_join = df.join(zones, df.PULocationID == zones.LocationID, "left")

# Count pickups per zone and order ascending
df_join.groupBy("Zone") \
    .agg(count("*").alias("pickup_count")) \
    .orderBy("pickup_count") \
    .show(1)
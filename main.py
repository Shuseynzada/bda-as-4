from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum as spark_sum
from pyspark.sql.window import Window
from geopy.distance import geodesic
from pyspark.sql.types import DoubleType
import os

# Initialize Spark session with increased memory settings
spark = SparkSession.builder \
    .appName("Maritime Data Analysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Load the data
file_path = "aisdk-2024-05-04.csv"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"The file {file_path} does not exist.")

df = spark.read.csv(file_path, header=True, inferSchema=True)

# Ensure data types are correct
df = df.withColumn("latitude", col("latitude").cast("double"))
df = df.withColumn("longitude", col("longitude").cast("double"))
df = df.withColumn("timestamp", col("# Timestamp").cast("timestamp"))

# Filter out rows with invalid latitude or longitude values
df = df.filter((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)))

# Cache the DataFrame to improve performance
df.cache()

# Define a UDF to calculate distance between two coordinates
def haversine(lat1, lon1, lat2, lon2):
    if None in [lat1, lon1, lat2, lon2]:
        return None
    return geodesic((lat1, lon1), (lat2, lon2)).nautical

from pyspark.sql.functions import udf

haversine_udf = udf(haversine, DoubleType())

# Create a window specification to partition by MMSI and order by timestamp
window_spec = Window.partitionBy("MMSI").orderBy("timestamp")

# Calculate the distance between consecutive positions
df = df.withColumn("prev_latitude", lag("latitude").over(window_spec))
df = df.withColumn("prev_longitude", lag("longitude").over(window_spec))
df = df.withColumn("distance", haversine_udf(col("latitude"), col("longitude"), col("prev_latitude"), col("prev_longitude")))

# Repartition the DataFrame to improve parallel processing
df = df.repartition("MMSI")

# Aggregate the distances by MMSI to get the total distance traveled by each vessel
df_distance = df.groupBy("MMSI").agg(spark_sum("distance").alias("total_distance"))

# Cache the DataFrame to avoid recomputation
df_distance.cache()

# Find the vessel that traveled the longest distance
longest_route = df_distance.orderBy(col("total_distance").desc()).first()


# Output the result
print(f"Vessel with MMSI {longest_route['MMSI']} traveled the longest distance: {longest_route['total_distance']} nautical miles")

# Stop the Spark session
spark.stop()

# Expected output Vessel with MMSI 219133000 traveled the longest distance: 5561067.58737947 nautical miles
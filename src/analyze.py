from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# while building we use one month only (jan 2024)
FIRST_DAY = "2024-01-01"
LAST_DAY = "2024-01-31"

weather = spark.read.parquet("data/parquet/weather")
weather.describe().show()
print("rainy days:", weather.where("prcp >= 10").count())
print("snow days:", weather.where("snow > 0").count())

###############################

days = (
    weather.select(
        "date",
        (F.col("prcp") / 10).alias("prcp_mm"),
        F.col("snow").alias("snow_mm"),
        (F.col("tmax") / 10).alias("tmax_c"),
        (F.col("tmin") / 10).alias("tmin_c"),
    )
    .withColumn("rain_bucket", F.when(F.col("prcp_mm") >= 1, "rain").otherwise("dry"))
    .withColumn("snow_bucket", F.when(F.col("snow_mm") > 0, "snow").otherwise("no_snow"))
    .withColumn("temp_bucket", F.when(F.col("tmax_c") < 5, "cold").when(F.col("tmax_c") >= 25, "hot").otherwise("mild"))
)
days.groupBy("rain_bucket").count().show()
days.groupBy("snow_bucket").count().show()
days.groupBy("temp_bucket").count().show()

################################

trips = spark.read.parquet("data/raw/trips/yellow_tripdata_2024-01.parquet").select(
    F.to_date("tpep_pickup_datetime").alias("pickup_date"),
    F.col("PULocationID"),
    ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60).alias("duration_min"),
    F.col("fare_amount"),
)
trips.createOrReplaceTempView("trips")

daily = spark.sql(f"""
    SELECT PULocationID AS zone_id, pickup_date,
           COUNT(*) AS trips, SUM(duration_min) AS sum_duration, SUM(fare_amount) AS sum_fare
    FROM trips
    WHERE pickup_date BETWEEN '{FIRST_DAY}' AND '{LAST_DAY}'
    GROUP BY PULocationID, pickup_date
""")
print("daily rows:", daily.count())
daily.show(5)

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

#####################

days.where(f"date BETWEEN '{FIRST_DAY}' AND '{LAST_DAY}'").createOrReplaceTempView("days")
daily.createOrReplaceTempView("daily")
spark.read.parquet("data/parquet/zones").createOrReplaceTempView("zones")


def by_zone(bucket_col):
    # trips per day = all trips of the bucket / number of days in the bucket
    # (we need per day because there are more dry days than snow days)
    return spark.sql(f"""
        SELECT z.Borough, z.Zone, d.{bucket_col} AS bucket,
               SUM(t.trips) / n.n_days AS trips_per_day,
               SUM(t.sum_duration) / SUM(t.trips) AS avg_duration_min,
               SUM(t.sum_fare) / SUM(t.trips) AS avg_fare
        FROM daily t
        JOIN days d ON t.pickup_date = d.date
        JOIN (SELECT {bucket_col} AS bucket, COUNT(*) AS n_days FROM days GROUP BY {bucket_col}) n
          ON n.bucket = d.{bucket_col}
        JOIN zones z ON z.LocationID = t.zone_id
        GROUP BY z.Borough, z.Zone, d.{bucket_col}, n.n_days
    """)


rain = by_zone("rain_bucket")
rain.orderBy(F.desc("trips_per_day")).show(6, truncate=False)

#####################

def compare(bucket_col, baseline, other):
    stats = by_zone(bucket_col)
    wide = stats.groupBy("Borough", "Zone").pivot("bucket", [baseline, other]).agg(F.first("trips_per_day"))
    return wide.withColumn("pct_change", F.round((F.col(other) / F.col(baseline) - 1) * 100, 1))


rain_change = compare("rain_bucket", "dry", "rain")
rain_change.where("dry >= 100").orderBy("pct_change").show(5, truncate=False)
rain_change.where("dry >= 100").orderBy(F.desc("pct_change")).show(5, truncate=False)


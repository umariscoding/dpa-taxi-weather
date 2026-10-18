import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# full data (jul 2023 to dec 2025)
FIRST_DAY = "2023-07-01"
LAST_DAY = "2025-12-31"

weather = spark.read.parquet("data/parquet/weather_clean")
weather.describe().show()
print("rainy days:", weather.where("prcp_mm >= 1").count())
print("snow days:", weather.where("snow_mm > 0").count())

days = (
    weather
    .withColumn("rain_bucket", F.when(F.col("prcp_mm") >= 1, "rain").otherwise("dry"))
    .withColumn("snow_bucket", F.when(F.col("snow_mm") > 0, "snow").otherwise("no_snow"))
    .withColumn("temp_bucket", F.when(F.col("tmax_c") < 5, "cold").when(F.col("tmax_c") >= 25, "hot").otherwise("mild"))
)

days.groupBy("rain_bucket").count().show()
days.groupBy("snow_bucket").count().show()
days.groupBy("temp_bucket").count().show()

################################

trips = spark.read.parquet("data/parquet/trips_clean")


trips.createOrReplaceTempView("trips")

daily = spark.sql(f"""
    SELECT PULocationID AS zone_id, pickup_date,
           COUNT(*) AS trips, SUM(duration_min) AS sum_duration, SUM(fare_amount) AS sum_fare
    FROM trips
    WHERE pickup_date BETWEEN '{FIRST_DAY}' AND '{LAST_DAY}'
    GROUP BY PULocationID, pickup_date
""").cache()
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
    wide = stats.groupBy("Borough", "Zone").pivot("bucket", [baseline, other]).agg(
        F.first("trips_per_day").alias("trips"),
        F.first("avg_duration_min").alias("duration"),
        F.first("avg_fare").alias("fare"),
    )

    def pct(metric):
        return F.round((F.col(f"{other}_{metric}") / F.col(f"{baseline}_{metric}") - 1) * 100, 1)

    # skip tiny zones (under 100 trips per day in the baseline) because they give silly percentages
    return (
        wide.where(F.col(f"{baseline}_trips") >= 100)
        .select(
            "Borough",
            "Zone",
            F.round(F.col(f"{baseline}_trips"), 1).alias("trips_per_day_base"),
            pct("trips").alias("trips_pct"),
            pct("duration").alias("duration_pct"),
            pct("fare").alias("fare_pct"),
        )
    )


factors = [
    ("rain", "rain_bucket", "dry", "rain"),
    ("snow", "snow_bucket", "no_snow", "snow"),
    ("cold", "temp_bucket", "mild", "cold"),
    ("hot", "temp_bucket", "mild", "hot"),
]

os.makedirs("outputs", exist_ok=True)
for name, bucket_col, baseline, other in factors:
    result = compare(bucket_col, baseline, other)
    print(name, "vs", baseline, "- biggest drops in trips per day")
    result.orderBy("trips_pct").show(5, truncate=False)
    print(name, "vs", baseline, "- biggest rises in trips per day")
    result.orderBy(F.desc("trips_pct")).show(5, truncate=False)
    result.toPandas().to_csv(f"outputs/{name}_change.csv", index=False)

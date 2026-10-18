import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("clean").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

OUT = "data/parquet"
os.makedirs("outputs", exist_ok=True)

# ---------- weather ----------
weather = spark.read.parquet(f"{OUT}/weather")
counts = [("weather start", weather.count())]

# missing values: count them per column, then drop those rows
weather.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in weather.columns]).show()
weather = weather.dropna()
counts.append(("drop rows with missing values", weather.count()))

# duplicates: we want one row per day
weather = weather.dropDuplicates(["date"])
counts.append(("drop duplicate days", weather.count()))

# noaa gives tenths, we want mm and celsius
weather = weather.select(
    "date",
    (F.col("prcp") / 10).alias("prcp_mm"),
    F.col("snow").alias("snow_mm"),
    (F.col("tmax") / 10).alias("tmax_c"),
    (F.col("tmin") / 10).alias("tmin_c"),
)

# impossible values
bad = "prcp_mm < 0 OR snow_mm < 0 OR tmax_c > 50 OR tmin_c < -40 OR tmin_c > tmax_c"
print("impossible rows:", weather.where(bad).count())
weather = weather.where(f"NOT ({bad})")
counts.append(("drop impossible values", weather.count()))

weather.write.mode("overwrite").parquet(f"{OUT}/weather_clean")

# row counts for the report
with open("outputs/cleaning_counts_weather.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["step", "rows_after"])
    writer.writerows(counts)
for step, rows in counts:
    print(step, rows)

# ---------- trips ----------
trips = spark.read.parquet(f"{OUT}/trips")
trips = trips.withColumn(
    "duration_min",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
)

# every rule keeps the good rows. unknown zones are 264 (Unknown) and 265 (Outside of NYC)
# we keep dropoff 265 because trips that end outside nyc are real (part 3 graph can use them)
rules = [
    ("pickup date inside jul 2023 - dec 2025", "tpep_pickup_datetime >= '2023-07-01' AND tpep_pickup_datetime < '2026-01-01'"),
    ("duration between 1 and 180 min", "duration_min >= 1 AND duration_min <= 180"),
    ("fare above 0 and max 500", "fare_amount > 0 AND fare_amount <= 500"),
    ("distance above 0 and max 100 miles", "trip_distance > 0 AND trip_distance <= 100"),
    ("pickup zone known (not 264, 265)", "PULocationID NOT IN (264, 265)"),
    ("dropoff zone known (not 264)", "DOLocationID != 264"),
]

# count rows after each rule in one pass over the data (rule 2 counts rows that pass rule 1 and 2 etc)
cumulative = []
aggregates = [F.count("*")]
for name, rule in rules:
    cumulative.append(f"({rule})")
    aggregates.append(F.sum(F.when(F.expr(" AND ".join(cumulative)), 1).otherwise(0)))
result = trips.agg(*aggregates).collect()[0]
trip_counts = [("trips start", result[0])]
for i, (name, rule) in enumerate(rules):
    trip_counts.append((name, result[i + 1]))

trips = trips.where(" AND ".join(f"({rule})" for name, rule in rules))

# exact duplicates: hash of the whole original row, keep one of each
all_columns = [c for c in trips.columns if c != "duration_min"]
trips = trips.withColumn("row_hash", F.xxhash64(*all_columns)).dropDuplicates(["row_hash"])

# only keep the columns we need
trips = trips.select(
    F.col("tpep_pickup_datetime").alias("pickup_ts"),
    F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
    F.to_date("tpep_pickup_datetime").alias("pickup_date"),
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "duration_min",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "passenger_count",
)
trips.write.mode("overwrite").parquet(f"{OUT}/trips_clean")
trip_counts.append(("drop duplicate rows", spark.read.parquet(f"{OUT}/trips_clean").count()))

# row counts for the report
with open("outputs/cleaning_counts_trips.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["step", "rows_after", "rows_removed"])
    for i, (step, rows) in enumerate(trip_counts):
        removed = 0 if i == 0 else trip_counts[i - 1][1] - rows
        writer.writerow([step, rows, removed])
        print(f"{step}: {rows} left, {removed} removed")

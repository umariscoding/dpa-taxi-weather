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

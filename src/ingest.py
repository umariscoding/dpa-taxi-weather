"""
Ingest: raw files -> typed parquet tables in data/parquet/

Reads data/raw/ with the fixed schemas from schema.py and writes three tables:
  zones    the taxi zone lookup
  weather  daily weather for our 915 days (values still in noaa units)
  trips    all 30 months of taxi trips

Run from the project folder inside docker:
  docker compose run --rm notebook python src/ingest.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schema import TRIPS_SCHEMA, WEATHER_SCHEMA, ZONES_SCHEMA

RAW = "data/raw"
OUT = "data/parquet"

# the 915 days we use (july 2023 to dec 2025)
FIRST_DAY = "2023-07-01"
LAST_DAY = "2025-12-31"

spark = (
    SparkSession.builder.master("local[*]").appName("ingest")
    .config("spark.ui.showConsoleProgress", "false")  # no progress bars in the output
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def ingest_trips():
    # one month at a time so the laptop memory is ok
    # only the parquet files, so a .DS_Store or a half downloaded .part file cannot break it
    files = sorted(f for f in os.listdir(f"{RAW}/trips") if f.endswith(".parquet"))
    if not files:
        raise SystemExit("no trip files in data/raw/trips, run src/download_data.py first")
    for i, name in enumerate(files):
        df = spark.read.schema(TRIPS_SCHEMA).parquet(f"{RAW}/trips/{name}")
        # first month starts the table (overwrite so rerun is fine), others get added
        mode = "overwrite" if i == 0 else "append"
        df.write.mode(mode).parquet(f"{OUT}/trips")
        print(f"trips {i + 1}/{len(files)}")
    print("trips rows:", spark.read.parquet(f"{OUT}/trips").count())


def ingest_zones():
    zones = spark.read.option("header", True).schema(ZONES_SCHEMA).csv(f"{RAW}/taxi_zone_lookup.csv")
    zones.write.mode("overwrite").parquet(f"{OUT}/zones")
    print("zones rows:", zones.count())


def ingest_weather():
    raw = spark.read.option("header", True).schema(WEATHER_SCHEMA).csv(f"{RAW}/weather_central_park.csv")
    # file goes back to 1869, keep only our days
    # values are still in noaa units here (PRCP, TMAX, TMIN in tenths), clean.py converts them
    weather = (
        raw.where(F.col("DATE").between(FIRST_DAY, LAST_DAY))
        .select(
            F.to_date("DATE").alias("date"),
            F.col("PRCP").cast("int").alias("prcp"),
            F.col("SNOW").cast("int").alias("snow"),
            F.col("TMAX").cast("int").alias("tmax"),
            F.col("TMIN").cast("int").alias("tmin"),
        )
    )
    weather.write.mode("overwrite").parquet(f"{OUT}/weather")
    print("weather rows:", weather.count())


ingest_zones()
ingest_weather()
ingest_trips()
print("ingest finished")

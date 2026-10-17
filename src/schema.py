"""
Schemas for the raw files (trips, zones, weather)

2025 trip files have one extra column, cbd_congestion_fee. Its in the schema
for all months, 2023 and 2024 just get null there
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

# types are same as in the parquet files, checked already
TRIPS_SCHEMA = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count", LongType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", LongType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", LongType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType()),
    StructField("cbd_congestion_fee", DoubleType()),
])

ZONES_SCHEMA = StructType([
    StructField("LocationID", IntegerType()),
    StructField("Borough", StringType()),
    StructField("Zone", StringType()),
    StructField("service_zone", StringType()),
])

# noaa file has ~120 columns but we need only a few
# csv columns are matched by position so we list the first 15 (until TMIN) in file order
# all strings here, ingest.py turns PRCP SNOW TMAX TMIN into numbers
WEATHER_SCHEMA = StructType([
    StructField("STATION", StringType()),
    StructField("DATE", StringType()),
    StructField("LATITUDE", StringType()),
    StructField("LONGITUDE", StringType()),
    StructField("ELEVATION", StringType()),
    StructField("NAME", StringType()),
    StructField("PRCP", StringType()),
    StructField("PRCP_ATTRIBUTES", StringType()),
    StructField("SNOW", StringType()),
    StructField("SNOW_ATTRIBUTES", StringType()),
    StructField("SNWD", StringType()),
    StructField("SNWD_ATTRIBUTES", StringType()),
    StructField("TMAX", StringType()),
    StructField("TMAX_ATTRIBUTES", StringType()),
    StructField("TMIN", StringType()),
])

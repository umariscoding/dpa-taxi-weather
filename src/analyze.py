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

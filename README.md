# Weather and taxi demand in New York City (Part 1, Spark SQL)

DISS-DPA Fall 2026. Group: Muhammad Umar Azhar and Wadan Ali.

This is part 1 (Volume) of our project. We use Spark SQL on about 100 million taxi trips and daily weather data.

**Main question:** which New York taxi zones are the most important, and does bad weather change taxi demand there more than past data predicts?

**Part 1 question:** how do rain, snow and temperature change trips, trip time and fares per zone?

Part 2 (streaming) and part 3 (graph) will use the same data later.

## Data

| Dataset | What it is | Size | Licence |
|---|---|---|---|
| [NYC yellow taxi trips](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | one parquet file per month, July 2023 to December 2025 (30 files, about 108.7 million trips) | 1.86 GB for all raw files | NYC Open Data terms of use |
| [Taxi zone lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) | 265 zones with borough and name | 12 KB | NYC Open Data terms of use |
| [NOAA daily weather, Central Park](https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW00094728.csv) | station USW00094728, we use 915 days (2023-07-01 to 2025-12-31) | 17.8 MB | NOAA public data |

Joins: trips and zones on the zone id, trips and weather on the pick up date.

Why Spark: 108 million rows are too much for pandas on a laptop (the data is more than 1.8 GB and expands a lot in memory). Spark reads it in parts and does the joins and group bys for us.

## What you need

- Docker Desktop (we used Mac and Windows)
- Docker memory of at least 6 GB (Docker Desktop > Settings > Resources)
- about 15 GB of free disk (raw data 1.9 GB, parquet files 4.6 GB, docker image 1.6 GB, and temp files from spark)
- Python 3 on your computer, only for the download script (it uses just the standard library)

## How to run everything

Run all commands from the project folder. Order matters, each step needs the one before it.

**1. Download the data** (about 15 minutes, depends on your internet)

```
python3 scripts/download_data.py
```

On Windows use `python` instead of `python3`. It makes `data/raw/` with 30 trip files, the zone csv and the weather csv. If a download fails just run it again, it skips files that are already there. You will see `All files downloaded.` at the end.

**2. Build the docker image** (once, a few minutes)

```
docker compose build
```

**3. Ingest** (about 1 minute)

```
docker compose run --rm spark python src/ingest.py
```

Reads the raw files with a fixed schema and writes typed parquet tables to `data/parquet/` (`trips`, `zones`, `weather`). At the end it prints:

```
zones rows: 265
weather rows: 915
trips rows: 108708928
ingest finished
```

**4. Clean** (about 2 minutes)

```
docker compose run --rm spark python src/clean.py
```

Writes `data/parquet/trips_clean` and `data/parquet/weather_clean`, and two files with row counts in `outputs/`. The last line says `drop duplicate rows: 101088746 left, 4 removed`.

**5. Analyze** (under 1 minute)

```
docker compose run --rm spark python src/analyze.py
```

Prints the biggest changes per weather type and saves four csv files in `outputs/`.

**6. Plots** (a few seconds). Run this after step 5, it reads the csv files from step 5.

```
docker compose run --rm spark python src/plots.py
```

Makes 12 pictures in `outputs/`.

We ran this whole list from an empty folder (no data, no outputs) and everything worked. Times above are from our laptops.

## What each step does

**Ingest** (`src/ingest.py`, schemas in `src/schema.py`). One fixed schema for all months. The 2025 files have one extra column (`cbd_congestion_fee`, the new congestion fee), the 2023 and 2024 files get null there. Trips are read one month at a time so memory stays low. For the weather we keep only date, rain, snow, max and min temperature for our 915 days.

**Clean** (`src/clean.py`). For the weather: we checked missing values, duplicate days and impossible values (nothing to remove) and changed units (noaa gives tenths, we want mm and celsius). For the trips we remove bad rows with these rules, in this order:

| Rule | Rows removed |
|---|---|
| pick up date outside July 2023 to December 2025 | 115 |
| trip time under 1 minute or over 180 minutes | 1,875,936 |
| fare 0 or less, or more than 500 dollars | 3,622,198 |
| distance 0 or less, or more than 100 miles | 1,593,787 |
| pick up zone unknown (264) or outside NYC (265) | 329,583 |
| drop off zone unknown (264) | 198,559 |
| exact duplicate rows | 4 |

We start with 108,708,928 trips and keep 101,088,746 (about 93%). The table is also in `outputs/cleaning_counts_trips.csv`.

Some choices: negative fares are credits, voids and adjustments (flex fare, dispute, cash, no charge) so they are not normal paid trips. About 13 million cleaned trips have no passenger count (flex fare trips), we keep them because we never use that column. We keep drop offs to zone 265 (outside NYC) because those trips are real.

**Analyze** (`src/analyze.py`). We label every day: rain (1 mm or more) or dry, snow (any snow) or no snow, cold (max temperature under 5 C), hot (25 C or more) or mild. Then for every pick up zone we count trips per day, average trip time and average fare for each type of day. We use trips per day (not total trips) because there are way more dry days than snow days. Then we compare to the normal day (dry, no snow, mild) in percent. Zones with less than 100 trips per day on normal days are left out, they give silly percentages. 66 busy zones are left.

## Output files

| File | What is inside |
|---|---|
| `outputs/cleaning_counts_trips.csv` and `cleaning_counts_weather.csv` | rows left and removed after every cleaning step |
| `outputs/rain_change.csv`, `snow_change.csv`, `cold_change.csv`, `hot_change.csv` | one row per zone: normal trips per day and the percent change in trips, trip time and fare |
| `outputs/<weather>_<measure>.png` | the 10 biggest drops and 10 biggest rises, for example `rain_trips_pct.png` or `snow_duration_pct.png` |

## Results in short

- **Rain:** small effect. Trips per day change between -7% and +7%, trip time and fares almost not at all. Financial District and the airports lose a bit, East Village and Lower East Side gain a bit.
- **Snow:** the biggest effect. LaGuardia and East Elmhurst lose 31% of trips, JFK 23%. Trip time is shorter in every busy zone (-6% to -20%). Some Queens zones and Alphabet City gain trips.
- **Cold:** the airports lose about 11 to 12%, Astoria gains 10%.
- **Hot:** Upper East Side and Upper West Side lose 15 to 19% of trips, Williamsburg gains about 19%.

Things to remember: there are only 22 snow days, the weather comes from one station (Central Park), and hot days are summer days and cold days are winter days, so temperature is mixed with the season. We compare averages, this does not prove the weather is the cause.

## Project folder

```
scripts/download_data.py    downloads the raw data
src/schema.py               fixed schemas
src/ingest.py               raw files to parquet
src/clean.py                cleaning and row counts
src/analyze.py              spark sql analysis
src/plots.py                pictures
Dockerfile, docker-compose.yml, requirements.txt    spark in docker
data/                       created by the scripts (not in git)
outputs/                    created by the scripts (not in git)
```

## If something goes wrong

- input/output error or docker stops: the disk is full. Free some space (Docker Desktop > Troubleshoot > Clean / Purge data helps) and build the image again.
- Killed or java memory errors: give docker more memory in the settings.
- `outputs/rain_change.csv` not found when running the plots: run step 5 (analyze) first.
- `trips_clean` or `weather_clean` not found: run steps 3 and 4 first.

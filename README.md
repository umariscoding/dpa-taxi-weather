# Weather and taxi demand in New York City (Part 1, Spark SQL)

DISS-DPA Fall 2026. Group: Muhammad Umar Azhar and Wadan Ali.

This is part 1 (Volume) of our project. We use Spark SQL on about 100 million taxi trips and daily weather data.

**Main question:** which New York taxi zones are the most important, and does bad weather change taxi demand there more than past data predicts?

**Part 1 question:** how do rain, snow and temperature change trips, trip time and fares per zone?

Part 2 (streaming) and part 3 (graph) will use the same data later.

## How it works (in short)

Four steps, one after the other. Steps 1 and 2 are python scripts, steps 3 and 4 are jupyter notebooks. Everything runs in docker.

| Step | What it does | Where the result goes |
|---|---|---|
| 1. `src/download_data.py` | downloads the data | `data/raw/` |
| 2. `src/ingest.py` | reads the raw files with a fixed schema and saves them as parquet (a faster format) | `data/parquet/` |
| 3. `notebooks/01_clean.ipynb` | removes the bad rows | clean tables in `data/parquet/`, row counts in `outputs/cleaning/` |
| 4. `notebooks/02_analysis.ipynb` | compares every weather type with a normal day and draws charts | tables in `outputs/results/`, charts in `outputs/charts/` |

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
- Docker memory of at least 6 GB (Docker Desktop > Settings > Resources). Spark uses 4 GB of it.
- about 20 GB of free disk (raw data 1.9 GB, parquet files 4.7 GB, docker image 6.6 GB, and temp files from spark)
- Python 3 on your computer, only for the download script (it uses just the standard library)

The docker setup is the one from the lab (the `jupyter/pyspark-notebook` image, Spark 3.5.0, Python 3.11, Java 17). The only differences: the project folder is shared with the container, so the data stays on your computer, and two settings for spark in `compose.yaml`.

## How to run everything

Run all commands from the project folder. Order matters, each step needs the one before it. **Run only one thing at a time.** Every spark job takes 4 GB of the docker memory, so two at the same time can crash (see the end of this file).

**1. Download the data** (about 15 minutes, depends on your internet)

```
python3 src/download_data.py
```

On Windows use `python` instead of `python3`. It makes `data/raw/` with 30 trip files, the zone csv and the weather csv. A failed file is tried again 4 times. If it still fails just run the command again, files that are already there are skipped. You will see `All files downloaded.` at the end.

**2. Build the docker image** (once, the image is about 2 GB to download, it can take long if your internet is slow)

```
docker compose build
```

**3. Ingest** (about 1 minute)

```
docker compose run --rm notebook python src/ingest.py
```

Reads the raw files with a fixed schema and writes typed parquet tables to `data/parquet/` (`trips`, `zones`, `weather`). It prints `trips 1/30` up to `trips 30/30` and at the end:

```
zones rows: 265
weather rows: 915
trips rows: 108708928
ingest finished
```

**4. Start jupyter**

```
docker compose up
```

Then open http://localhost:8889 in your browser (no password, like in the lab). Stop it later with Ctrl+C in the terminal.

**5. Notebook 1: cleaning** (about 3 minutes)

Open `notebooks/01_clean.ipynb` and choose *Run > Run All Cells*. You can also run the cells one by one. Every cell shows what it did. A cell with `[*]` is still working, wait until the status at the bottom left says `Idle`. It writes `data/parquet/trips_clean` and `data/parquet/weather_clean`, and two files with row counts in `outputs/cleaning/`. The last output is a table of all cleaning steps, ending with `drop duplicate rows` and 101088746 rows left.

**6. Notebook 2: analysis and plots** (under 1 minute)

Open `notebooks/02_analysis.ipynb` and choose *Run > Run All Cells*. It saves four csv files in `outputs/results/` and 12 pictures in `outputs/charts/`. The charts for trips per day are also shown in the notebook.

We ran all the steps from a clean state (no parquet files, no outputs) and everything worked. Times are from our laptops.

## What each step does

**Ingest** (`src/ingest.py`, schemas in `src/schema.py`). One fixed schema for all months. The 2025 files have one extra column (`cbd_congestion_fee`, the new congestion fee), the 2023 and 2024 files get null there. Trips are read one month at a time so memory stays low. For the weather we keep only date, rain, snow, max and min temperature for our 915 days.

**Clean** (`notebooks/01_clean.ipynb`). For the weather: we checked missing values, duplicate days and impossible values (nothing to remove) and changed units (noaa gives tenths, we want mm and celsius). For the trips we remove bad rows with these rules, in this order:

| Rule | Rows removed |
|---|---|
| pick up date outside July 2023 to December 2025 | 115 |
| trip time under 1 minute or over 180 minutes | 1,875,936 |
| fare 0 or less, or more than 500 dollars | 3,622,198 |
| distance 0 or less, or more than 100 miles | 1,593,787 |
| pick up zone unknown (264) or outside NYC (265) | 329,583 |
| drop off zone unknown (264) | 198,559 |
| exact duplicate rows | 4 |

We start with 108,708,928 trips and keep 101,088,746 (about 93%). The table is also in `outputs/cleaning/trips.csv`.

Some choices: negative fares are credits, voids and adjustments (flex fare, dispute, cash, no charge) so they are not normal paid trips. About 13 million cleaned trips have no passenger count (flex fare trips), we keep them because we never use that column. We keep drop offs to zone 265 (outside NYC) because those trips are real.

**Analysis and plots** (`notebooks/02_analysis.ipynb`). We label every day: rain (1 mm or more) or dry, snow (any snow) or no snow, cold (max temperature under 5 C), hot (25 C or more) or mild. Then for every pick up zone we count trips per day, average trip time and average fare for each type of day. We use trips per day (not total trips) because there are way more dry days than snow days. Then we compare to the normal day (dry, no snow, mild) in percent. Zones with less than 100 trips per day on normal days are left out, they give silly percentages. 66 busy zones are left (96.5% of the trips).

## Output files

| File | What is inside |
|---|---|
| `outputs/cleaning/weather.csv` and `trips.csv` | rows left and removed after every cleaning step |
| `outputs/results/rain_change.csv`, `snow_change.csv`, `cold_change.csv`, `hot_change.csv` | one row per zone: normal trips per day and the percent change in trips, trip time and fare |
| `outputs/charts/<weather>_<measure>.png` | the 10 biggest drops and 10 biggest rises, for example `rain_trips_pct.png` or `snow_duration_pct.png` (12 charts) |

## Results in short

- **Rain:** small effect. Trips per day change between -7% and +7%, trip time and fares almost not at all. Financial District and the airports lose a bit, East Village and Lower East Side gain a bit.
- **Snow:** the biggest effect. LaGuardia and East Elmhurst lose 31% of trips, JFK 23%. Trip time is shorter in every busy zone (-6% to -20%). Some Queens zones and Alphabet City gain trips.
- **Cold:** the airports lose about 11 to 12%, Astoria gains 10%.
- **Hot:** Upper East Side and Upper West Side lose 15 to 19% of trips, Williamsburg gains about 19%.

Things to remember: there are only 22 snow days, the weather comes from one station (Central Park), and hot days are summer days and cold days are winter days, so temperature is mixed with the season. We compare averages, this does not prove the weather is the cause.

## Project folder

```
src/download_data.py          step 1: downloads the raw data
src/schema.py                 the fixed schemas (used by ingest)
src/ingest.py                 step 2: raw files to parquet
notebooks/01_clean.ipynb      step 3: cleaning and row counts
notebooks/02_analysis.ipynb   step 4: analysis and charts
Dockerfile, compose.yaml      jupyter with spark in docker (from the lab)
data/                         created by steps 1 to 3 (not in git)
outputs/                      created by the notebooks (not in git)
report/                       the report
```

## If something goes wrong

- input/output error or docker stops: the disk is full. Free some space (Docker Desktop > Troubleshoot > Clean / Purge data helps) and build the image again.
- Killed or java memory errors: give docker more memory in the settings.
- "Answer from Java side is empty" or "file does not exist" in a notebook: two things were running at the same time (for example a notebook and `ingest.py`, or two notebooks). Stop everything with `docker compose down` and run one step at a time.
- `trips_clean` or `weather_clean` not found in notebook 2: run notebook 1 first. `trips` or `weather` not found in notebook 1: run step 3 (ingest) first.
- `outputs/results/rain_change.csv` not found (when you look for it): run notebook 2 first.

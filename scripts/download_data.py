"""
Download the raw data for the project into data/raw/.

  1. NYC TLC yellow taxi trips, one Parquet file per month
  2. TLC taxi zone lookup (CSV)
  3. NOAA daily weather for Central Park (CSV)

Usage:
  python scripts/download_data.py

Only the standard library is used. Files that already exist are skipped,
so it is safe to run again if a download fails.
"""

import argparse
import os
import time
import urllib.request

TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
WEATHER_URL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW00094728.csv"

# data/raw next to this script's parent folder, so it works from any directory
DEFAULT_OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw")

RETRIES = 4  # how many times we try again when a download fails
WAIT_SECONDS = 5  # wait before each retry


def list_months(start, end):
    """'2024-01', '2024-03' -> ['2024-01', '2024-02', '2024-03']"""
    year, month = int(start[:4]), int(start[5:])
    end_year, end_month = int(end[:4]), int(end[5:])
    months = []
    while (year, month) <= (end_year, end_month):
        months.append(f"{year}-{month:02d}")
        month += 1
        if month == 13:
            month = 1
            year += 1
    return months


def download(url, path):
    """Download url to path, unless the file is already there. Retries when it fails."""
    name = os.path.basename(path)
    if os.path.exists(path):
        print(f"skip {name} (already downloaded)")
        return True
    print(f"downloading {name} ...")
    for attempt in range(RETRIES + 1):
        if attempt > 0:
            print(f"  retry {attempt}/{RETRIES} for {name} in {WAIT_SECONDS} seconds ...")
            time.sleep(WAIT_SECONDS)
        try:
            urllib.request.urlretrieve(url, path + ".part")
            os.rename(path + ".part", path)  # only rename when the download finished
            return True
        except Exception as error:
            print(f"  failed {name}: {error}")
            if os.path.exists(path + ".part"):
                os.remove(path + ".part")
    print(f"  giving up on {name}")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2023-07", help="first month (YYYY-MM)")
    parser.add_argument("--end", default="2025-12", help="last month (YYYY-MM)")
    parser.add_argument("--out", default=DEFAULT_OUT, help="output folder")
    args = parser.parse_args()

    trips_folder = os.path.join(args.out, "trips")
    os.makedirs(trips_folder, exist_ok=True)

    failed = 0
    for month in list_months(args.start, args.end):
        ok = download(TRIP_URL.format(month=month), os.path.join(trips_folder, f"yellow_tripdata_{month}.parquet"))
        if not ok:
            failed += 1
    if not download(ZONE_URL, os.path.join(args.out, "taxi_zone_lookup.csv")):
        failed += 1
    if not download(WEATHER_URL, os.path.join(args.out, "weather_central_park.csv")):
        failed += 1

    if failed:
        print(f"\n{failed} download(s) failed. Run the script again to retry them.")
    else:
        print("\nAll files downloaded.")


if __name__ == "__main__":
    main()

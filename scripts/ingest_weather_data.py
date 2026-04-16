import requests
import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "shark-encounter-prod")
DATASET = "shark_raw"
TABLE = "bom_rainfall"

# BOM stations near popular surf/swim spots
# station numbers from http://www.bom.gov.au/climate/data/
STATIONS = {
    "058198": "Ballina Airport",
    "040913": "Gold Coast Seaway",
    "023034": "Adelaide (Kent Town)",
    "009021": "Perth Airport",
    "066062": "Sydney Observatory Hill",
    "031011": "Cairns Aero",
    "029009": "Hobart (Ellerslie Road)",
    "014015": "Darwin Airport",
}

BOM_BASE_URL = "http://www.bom.gov.au/fwo"


def fetch_rainfall_for_station(station_id, station_name):
    """
    Pull recent daily rainfall obs from BOM.
    Uses the JSON feed endpoint for the given station.
    """
    url = f"{BOM_BASE_URL}/IDN60801/IDN60801.{station_id}.json"

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        obs = data["observations"]["data"]
        records = []
        for o in obs:
            records.append({
                "station_id": station_id,
                "station_name": station_name,
                "observation_date": o.get("local_date_time_full", ""),
                "rainfall_mm": o.get("rain_trace", 0),
                "max_temp_c": o.get("air_temp", None),
            })

        return pd.DataFrame(records)

    except Exception as e:
        print(f"Could not fetch data for {station_name} ({station_id}): {e}")
        return pd.DataFrame()


def load_all_stations():
    frames = []
    for sid, name in STATIONS.items():
        print(f"Fetching {name}...")
        df = fetch_rainfall_for_station(sid, name)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("No weather data retrieved. Check BOM endpoints.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["rainfall_mm"] = pd.to_numeric(combined["rainfall_mm"], errors="coerce")
    combined["observation_date"] = pd.to_datetime(combined["observation_date"], errors="coerce")

    combined.to_csv("data/processed/bom_rainfall.csv", index=False)
    print(f"Saved {len(combined)} weather observations")
    return combined


def upload_to_bigquery(df):
    if df.empty:
        print("Nothing to upload")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Uploaded {job.output_rows} rows to {table_ref}")


if __name__ == "__main__":
    weather_df = load_all_stations()
    upload_to_bigquery(weather_df)

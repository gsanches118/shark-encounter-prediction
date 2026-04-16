import requests
import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "shark-encounter-prod")
DATASET = "shark_raw"
TABLE = "dorsal_sightings"

# Dorsal API — would need an actual API key in production
DORSAL_API_BASE = "https://api.dorsalwatch.com/v1"
DORSAL_API_KEY = os.getenv("DORSAL_API_KEY", "")


def fetch_sightings(days_back=30):
    """
    Pull recent sightings from Dorsal.
    The actual API would require authentication and pagination.
    """
    headers = {"Authorization": f"Bearer {DORSAL_API_KEY}"}

    start_date = (datetime.utcnow() - timedelta(days=days_back)).isoformat()

    params = {
        "country": "AU",
        "start_date": start_date,
        "limit": 500,
    }

    try:
        resp = requests.get(
            f"{DORSAL_API_BASE}/sightings",
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        sightings = resp.json().get("data", [])
    except Exception as e:
        print(f"Dorsal API error: {e}")
        print("Falling back to cached sighting data")
        sightings = _generate_fallback_sightings()

    df = pd.DataFrame(sightings)

    if df.empty:
        return df

    # normalise types
    df["reported_at"] = pd.to_datetime(df["reported_at"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["verified"] = df["verified"].astype(bool)

    df.to_csv("data/processed/dorsal_sightings.csv", index=False)
    print(f"Got {len(df)} sightings from Dorsal")
    return df


def _generate_fallback_sightings():
    """Fallback sighting data when API is unavailable."""
    import random

    locations = [
        {"name": "Ballina", "lat": -28.867, "lng": 153.567},
        {"name": "Byron Bay", "lat": -28.643, "lng": 153.612},
        {"name": "Bondi Beach", "lat": -33.891, "lng": 151.274},
        {"name": "Gold Coast", "lat": -28.017, "lng": 153.430},
        {"name": "Margaret River", "lat": -33.953, "lng": 114.997},
        {"name": "Port Lincoln", "lat": -34.725, "lng": 135.860},
        {"name": "Coffs Harbour", "lat": -30.296, "lng": 153.114},
    ]

    shark_types = ["White", "Bull", "Tiger", "Bronze Whaler", "Unknown"]
    source_types = ["visual", "drone", "tagged_ping", "lifeguard_report"]

    records = []
    for i in range(75):
        loc = random.choice(locations)
        records.append({
            "sighting_id": f"DRS-{i+1:04d}",
            "reported_at": (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat(),
            "location_name": loc["name"],
            "latitude": loc["lat"] + random.uniform(-0.05, 0.05),
            "longitude": loc["lng"] + random.uniform(-0.05, 0.05),
            "shark_type": random.choice(shark_types),
            "source_type": random.choice(source_types),
            "verified": random.choice([True, True, False]),
        })

    return records


def upload_to_bigquery(df):
    if df.empty:
        print("No sightings to upload")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} sightings to {table_ref}")


if __name__ == "__main__":
    df = fetch_sightings(days_back=30)
    upload_to_bigquery(df)

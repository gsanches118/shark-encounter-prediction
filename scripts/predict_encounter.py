import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "shark-encounter-prod")


def get_risk_data():
    """Pull the final risk scores from BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    query = """
    SELECT *
    FROM `shark-encounter-prod.shark_marts.shark_encounter_risk`
    ORDER BY risk_score DESC
    """

    try:
        df = client.query(query).to_dataframe()
    except Exception:
        print("BigQuery unavailable, using local prediction")
        df = _predict_from_local()

    return df


def _predict_from_local():
    """
    Run prediction locally when BQ isn't available.
    Uses processed CSVs if they exist.
    """
    # try to load processed data
    incidents_path = "data/processed/shark_incidents_clean.csv"
    sightings_path = "data/processed/dorsal_sightings.csv"
    weather_path = "data/processed/bom_rainfall.csv"

    locations = [
        {"location_name": "Ballina", "state": "New South Wales", "lat": -28.867, "lng": 153.567},
        {"location_name": "Byron Bay", "state": "New South Wales", "lat": -28.643, "lng": 153.612},
        {"location_name": "Bondi Beach", "state": "New South Wales", "lat": -33.891, "lng": 151.274},
        {"location_name": "Gold Coast", "state": "Queensland", "lat": -28.017, "lng": 153.430},
        {"location_name": "Noosa", "state": "Queensland", "lat": -26.39, "lng": 153.09},
        {"location_name": "Margaret River", "state": "Western Australia", "lat": -33.953, "lng": 114.997},
        {"location_name": "Port Lincoln", "state": "South Australia", "lat": -34.725, "lng": 135.860},
        {"location_name": "Coffs Harbour", "state": "New South Wales", "lat": -30.296, "lng": 153.114},
        {"location_name": "Manly Beach", "state": "New South Wales", "lat": -33.797, "lng": 151.288},
        {"location_name": "Torquay", "state": "Victoria", "lat": -38.332, "lng": 144.326},
    ]

    # count historical incidents per location if data exists
    incident_counts = {}
    if os.path.exists(incidents_path):
        incidents = pd.read_csv(incidents_path)
        if "location" in incidents.columns:
            incident_counts = incidents["location"].value_counts().to_dict()

    # count recent sightings
    sighting_counts = {}
    if os.path.exists(sightings_path):
        sightings = pd.read_csv(sightings_path)
        if "location_name" in sightings.columns:
            sighting_counts = sightings["location_name"].value_counts().to_dict()

    # average rainfall
    avg_rain = 0
    if os.path.exists(weather_path):
        weather = pd.read_csv(weather_path)
        if "rainfall_mm" in weather.columns:
            avg_rain = weather["rainfall_mm"].mean()

    now = datetime.now()
    month = now.month
    hour = now.hour
    is_whale_season = 5 <= month <= 11

    time_of_day = "midday"
    if 5 <= hour <= 8:
        time_of_day = "dawn"
    elif 16 <= hour <= 19:
        time_of_day = "dusk"
    elif hour >= 20 or hour < 5:
        time_of_day = "night"

    results = []
    for loc in locations:
        name = loc["location_name"]

        hist_count = incident_counts.get(name, 0)
        sightings_7d = sighting_counts.get(name, 0)
        rainfall = avg_rain if avg_rain > 0 else 0

        # same scoring logic as the SQL model
        score = min(sightings_7d * 15, 40)

        if hist_count > 20:
            score += 20
        elif hist_count > 10:
            score += 12
        elif hist_count > 5:
            score += 6
        else:
            score += 2

        if rainfall > 50:
            score += 15
        elif rainfall > 20:
            score += 8

        if is_whale_season:
            score += 10

        if time_of_day in ("dawn", "dusk"):
            score += 10
        elif time_of_day == "night":
            score += 5

        risk_level = "Low"
        if score >= 50:
            risk_level = "High"
        elif score >= 25:
            risk_level = "Medium"

        results.append({
            "location_name": name,
            "state": loc["state"],
            "score_date": now.strftime("%Y-%m-%d"),
            "risk_score": round(score, 1),
            "risk_level": risk_level,
            "recent_sightings_7d": sightings_7d,
            "avg_rainfall_7d_mm": round(rainfall, 1),
            "is_whale_season": is_whale_season,
            "time_of_day_risk": time_of_day,
            "historical_incident_count": hist_count,
        })

    return pd.DataFrame(results).sort_values("risk_score", ascending=False)


if __name__ == "__main__":
    results = get_risk_data()
    print("\n=== Shark Encounter Risk Scores ===\n")
    print(results.to_string(index=False))
    results.to_csv("data/processed/risk_scores.csv", index=False)
    print("\nSaved to data/processed/risk_scores.csv")

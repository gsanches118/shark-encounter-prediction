# Shark Encounter Risk Prediction — Australia

Predicting the probability of a shark encounter (Low / Medium / High) for surfers, swimmers and ocean enthusiasts along the Australian coastline.

## Overview

This project uses historical shark incident data from the [Australian Shark-Incident Database](https://www.taronga.org.au/conservation-and-science/australian-shark-incident-database) combined with environmental variables (rainfall, seasonality, time of day, whale migration patterns) and real-time sighting data from the [Dorsal](https://www.dorsalwatch.com/) app to produce a risk score for popular beach locations.

The pipeline is built on Google Cloud Platform using **BigQuery** for warehousing and analytics, and **Cloud Composer (Airflow)** for orchestration.

## Project Structure

```
shark-encounter-project/
├── data/
│   ├── raw/                        # Raw source files (xlsx, csvs)
│   └── processed/                  # Cleaned and prepped datasets
├── sql/
│   ├── staging/                    # Staging models — light cleaning
│   ├── intermediate/               # Joins, business logic
│   └── marts/                      # Final tables for analysis
├── dags/
│   └── shark_encounter_dag.py      # Airflow DAG definition
├── scripts/
│   ├── ingest_shark_data.py        # Load shark incident data to BQ
│   ├── ingest_weather_data.py      # Pull BOM rainfall data
│   ├── ingest_dorsal_data.py       # Dorsal sightings ingestion
│   └── predict_encounter.py        # Risk scoring model
├── outputs/
│   └── generate_report.py          # Generates the PDF report
├── config/
│   └── schema.yml                  # BigQuery table schemas
├── requirements.txt
└── README.md
```

## Data Sources

| Source | Description | Format |
|--------|-------------|--------|
| Australian Shark-Incident Database | Historical shark incidents (1791–present) | `.xlsx` |
| Bureau of Meteorology (BOM) | Daily rainfall observations | API / CSV |
| Dorsal Watch | Community shark sightings & tagged shark pings | API |

## Key Variables

- **Rainfall** — high rainfall in prior 1-7 days (runoff attracts baitfish)
- **Time of day** — dawn/dusk vs midday
- **Location** — hotspot areas like Ballina, Byron Bay, South Australia
- **Season** — winter months correlate with whale migration, which attracts sharks
- **Recent sightings** — Dorsal app reports in the area

## Risk Output

Each beach location gets a risk classification:

| Level | Description |
|-------|-------------|
| 🟢 Low | No recent sightings, low-risk conditions |
| 🟡 Medium | Some risk factors present (e.g. heavy rain, dusk) |
| 🔴 High | Multiple risk factors or recent confirmed sighting |

## Tech Stack

- **Google BigQuery** — data warehouse
- **Cloud Composer / Apache Airflow** — pipeline orchestration
- **Python** — ingestion scripts, model
- **FPDF** — PDF report generation

## Potential Integrations

The risk output could plug into:
- **Surfline** — show risk level alongside surf forecasts
- **Gold Coast City Council** — beach signage and lifeguard alerts
- **Strava / Garmin** — warn ocean swimmers before sessions
- **Dorsal app** — feed predictions back into the sighting platform

## How to Run

1. Set up a GCP project and enable BigQuery API
2. Install dependencies: `pip install -r requirements.txt`
3. Place raw data in `data/raw/`
4. Run ingestion scripts: `python scripts/ingest_shark_data.py`
5. Deploy the Airflow DAG to Cloud Composer
6. Generate report: `python outputs/generate_report.py`

## Author

Built by Guilherme — analytics engineering portfolio project.

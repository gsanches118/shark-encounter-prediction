from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags"), "scripts"))

default_args = {
    "owner": "analytics-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "shark_encounter_pipeline",
    default_args=default_args,
    description="Ingest data, run transformations, and score shark encounter risk",
    schedule_interval="0 6 * * *",  # daily at 6am AEST-ish
    start_date=days_ago(1),
    catchup=False,
    tags=["shark", "analytics"],
)

PROJECT_ID = "shark-encounter-prod"
LOCATION = "australia-southeast1"


# --- ingestion tasks ---

def _ingest_shark_data():
    from ingest_shark_data import load_shark_data, upload_to_bigquery
    df = load_shark_data()
    upload_to_bigquery(df)

def _ingest_weather():
    from ingest_weather_data import load_all_stations, upload_to_bigquery
    df = load_all_stations()
    upload_to_bigquery(df)

def _ingest_dorsal():
    from ingest_dorsal_data import fetch_sightings, upload_to_bigquery
    df = fetch_sightings(days_back=7)
    upload_to_bigquery(df)


ingest_sharks = PythonOperator(
    task_id="ingest_shark_incidents",
    python_callable=_ingest_shark_data,
    dag=dag,
)

ingest_weather = PythonOperator(
    task_id="ingest_weather_data",
    python_callable=_ingest_weather,
    dag=dag,
)

ingest_dorsal = PythonOperator(
    task_id="ingest_dorsal_sightings",
    python_callable=_ingest_dorsal,
    dag=dag,
)


# --- SQL transformation tasks ---

def read_sql_file(filename):
    sql_path = os.path.join(os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags"), "sql", filename)
    with open(sql_path, "r") as f:
        return f.read()


stg_incidents = BigQueryInsertJobOperator(
    task_id="stg_shark_incidents",
    configuration={
        "query": {
            "query": read_sql_file("staging/stg_shark_incidents.sql"),
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": "shark_staging",
                "tableId": "stg_shark_incidents",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
    location=LOCATION,
    dag=dag,
)

stg_weather = BigQueryInsertJobOperator(
    task_id="stg_weather",
    configuration={
        "query": {
            "query": read_sql_file("staging/stg_weather.sql"),
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": "shark_staging",
                "tableId": "stg_weather",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
    location=LOCATION,
    dag=dag,
)

stg_dorsal = BigQueryInsertJobOperator(
    task_id="stg_dorsal_sightings",
    configuration={
        "query": {
            "query": read_sql_file("staging/stg_dorsal_sightings.sql"),
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": "shark_staging",
                "tableId": "stg_dorsal_sightings",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
    location=LOCATION,
    dag=dag,
)

int_risk_factors = BigQueryInsertJobOperator(
    task_id="int_location_risk_factors",
    configuration={
        "query": {
            "query": read_sql_file("intermediate/int_location_risk_factors.sql"),
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": "shark_intermediate",
                "tableId": "int_location_risk_factors",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
    location=LOCATION,
    dag=dag,
)

mart_risk = BigQueryInsertJobOperator(
    task_id="shark_encounter_risk",
    configuration={
        "query": {
            "query": read_sql_file("marts/shark_encounter_risk.sql"),
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": "shark_marts",
                "tableId": "shark_encounter_risk",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
    location=LOCATION,
    dag=dag,
)


# --- task dependencies ---
# ingestion can run in parallel, then staging, then intermediate, then mart

[ingest_sharks, ingest_weather, ingest_dorsal] >> stg_incidents
[ingest_sharks, ingest_weather, ingest_dorsal] >> stg_weather
[ingest_sharks, ingest_weather, ingest_dorsal] >> stg_dorsal

[stg_incidents, stg_weather, stg_dorsal] >> int_risk_factors >> mart_risk

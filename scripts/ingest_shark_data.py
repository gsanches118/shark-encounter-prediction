import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "shark-encounter-prod")
DATASET = "shark_raw"
TABLE = "shark_incidents"
RAW_FILE = "data/raw/Australian Shark-Incident Database Public Version.xlsx"


def load_shark_data():
    """Read the xlsx and do basic cleanup before loading to BQ."""

    df = pd.read_excel(RAW_FILE, engine="openpyxl")

    # standardise column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "", regex=True)
    )

    # drop rows with no date at all
    df = df.dropna(subset=["incident_year"])

    # build a proper date column where possible
    if "incident_month" in df.columns and "incident_day" in df.columns:
        df["incident_date"] = pd.to_datetime(
            dict(
                year=df["incident_year"],
                month=df["incident_month"].fillna(1).astype(int),
                day=df["incident_day"].fillna(1).astype(int),
            ),
            errors="coerce",
        )
    else:
        df["incident_date"] = pd.to_datetime(df["incident_year"], format="%Y", errors="coerce")

    # clean up state names
    state_map = {
        "NSW": "New South Wales",
        "QLD": "Queensland",
        "VIC": "Victoria",
        "SA": "South Australia",
        "WA": "Western Australia",
        "TAS": "Tasmania",
        "NT": "Northern Territory",
    }
    if "state" in df.columns:
        df["state"] = df["state"].str.strip().replace(state_map)

    # basic type fix
    for col in ["latitude", "longitude", "shark_length_m"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_csv("data/processed/shark_incidents_clean.csv", index=False)
    print(f"Cleaned {len(df)} records -> data/processed/shark_incidents_clean.csv")

    return df


def upload_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows to {table_ref}")


if __name__ == "__main__":
    df = load_shark_data()
    upload_to_bigquery(df)

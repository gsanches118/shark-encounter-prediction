-- stg_shark_incidents.sql
-- Clean up raw shark incident data from the Australian Shark-Incident Database

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY incident_year, state) AS STRING) AS incident_id,
    SAFE.PARSE_DATE('%Y-%m-%d', CAST(incident_date AS STRING)) AS incident_date,
    CAST(incident_year AS INT64) AS incident_year,
    CAST(incident_month AS INT64) AS incident_month,
    TRIM(state) AS state,
    TRIM(location) AS location,
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) AS longitude,
    LOWER(TRIM(shark_common_name)) AS shark_species,
    LOWER(TRIM(victim_activity)) AS victim_activity,
    LOWER(TRIM(injury_severity)) AS injury_severity,
    CASE
        WHEN LOWER(TRIM(fatal)) IN ('y', 'yes', 'true') THEN TRUE
        ELSE FALSE
    END AS fatal,
    CASE
        WHEN CAST(incident_time AS INT64) BETWEEN 500 AND 900 THEN 'dawn'
        WHEN CAST(incident_time AS INT64) BETWEEN 901 AND 1600 THEN 'midday'
        WHEN CAST(incident_time AS INT64) BETWEEN 1601 AND 1900 THEN 'dusk'
        ELSE 'night'
    END AS time_of_day,
    CAST(shark_length_m AS FLOAT64) AS shark_length_m

FROM `shark-encounter-prod.shark_raw.shark_incidents`
WHERE incident_year IS NOT NULL

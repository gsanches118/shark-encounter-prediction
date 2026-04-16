-- stg_weather.sql
-- Staging model for BOM rainfall observations

SELECT
    station_id,
    station_name,
    SAFE.PARSE_DATE('%Y-%m-%d', CAST(observation_date AS STRING)) AS observation_date,
    CAST(rainfall_mm AS FLOAT64) AS rainfall_mm,
    CAST(max_temp_c AS FLOAT64) AS max_temp_c,
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) AS longitude

FROM `shark-encounter-prod.shark_raw.bom_rainfall`
WHERE observation_date IS NOT NULL

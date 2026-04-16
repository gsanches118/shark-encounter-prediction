-- stg_dorsal_sightings.sql
-- Clean Dorsal app sighting reports

SELECT
    sighting_id,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', reported_at) AS reported_at,
    TRIM(location_name) AS location_name,
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) AS longitude,
    LOWER(TRIM(shark_type)) AS shark_type,
    LOWER(TRIM(source_type)) AS source_type,
    CAST(verified AS BOOL) AS verified

FROM `shark-encounter-prod.shark_raw.dorsal_sightings`
WHERE sighting_id IS NOT NULL

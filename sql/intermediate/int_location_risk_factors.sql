-- int_location_risk_factors.sql
-- Bring together all risk factors per location per day

WITH locations AS (
    SELECT DISTINCT
        location AS location_name,
        state,
        latitude,
        longitude
    FROM `shark-encounter-prod.shark_staging.stg_shark_incidents`
    WHERE latitude IS NOT NULL
),

historical_counts AS (
    SELECT
        location,
        state,
        COUNT(*) AS total_incidents,
        COUNTIF(fatal) AS fatal_incidents,
        COUNTIF(victim_activity = 'surfing') AS surfing_incidents,
        COUNTIF(victim_activity = 'swimming') AS swimming_incidents
    FROM `shark-encounter-prod.shark_staging.stg_shark_incidents`
    GROUP BY location, state
),

recent_sightings AS (
    SELECT
        location_name,
        COUNT(*) AS sightings_7d,
        COUNTIF(verified) AS verified_sightings_7d
    FROM `shark-encounter-prod.shark_staging.stg_dorsal_sightings`
    WHERE reported_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY location_name
),

rainfall_7d AS (
    -- match weather stations to locations within ~50km
    SELECT
        l.location_name,
        AVG(w.rainfall_mm) AS avg_rainfall_7d_mm,
        MAX(w.rainfall_mm) AS max_rainfall_7d_mm
    FROM locations l
    JOIN `shark-encounter-prod.shark_staging.stg_weather` w
        ON ST_DISTANCE(
            ST_GEOGPOINT(l.longitude, l.latitude),
            ST_GEOGPOINT(w.longitude, w.latitude)
        ) < 50000  -- 50km radius
    WHERE w.observation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY l.location_name
)

SELECT
    l.location_name,
    l.state,
    l.latitude,
    l.longitude,
    CURRENT_DATE() AS score_date,

    COALESCE(hc.total_incidents, 0) AS historical_incident_count,
    COALESCE(hc.fatal_incidents, 0) AS fatal_incident_count,

    COALESCE(rs.sightings_7d, 0) AS recent_sightings_7d,
    COALESCE(rs.verified_sightings_7d, 0) AS verified_sightings_7d,

    COALESCE(r.avg_rainfall_7d_mm, 0) AS avg_rainfall_7d_mm,
    COALESCE(r.max_rainfall_7d_mm, 0) AS max_rainfall_7d_mm,

    -- whale season: roughly May to November in eastern Australia
    EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 5 AND 11 AS is_whale_season,

    -- time of day risk (simplified — in practice would use current time)
    CASE
        WHEN EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) BETWEEN 5 AND 8 THEN 'dawn'
        WHEN EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) BETWEEN 16 AND 19 THEN 'dusk'
        WHEN EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) BETWEEN 9 AND 15 THEN 'midday'
        ELSE 'night'
    END AS current_time_of_day

FROM locations l
LEFT JOIN historical_counts hc ON l.location_name = hc.location AND l.state = hc.state
LEFT JOIN recent_sightings rs ON l.location_name = rs.location_name
LEFT JOIN rainfall_7d r ON l.location_name = r.location_name

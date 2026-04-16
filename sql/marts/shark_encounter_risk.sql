-- shark_encounter_risk.sql
-- Final risk scoring model
-- Assigns Low / Medium / High based on weighted factors

WITH risk_factors AS (
    SELECT * FROM `shark-encounter-prod.shark_intermediate.int_location_risk_factors`
),

scored AS (
    SELECT
        location_name,
        state,
        score_date,
        historical_incident_count,
        recent_sightings_7d,
        avg_rainfall_7d_mm,
        is_whale_season,
        current_time_of_day,

        -- weighted risk score (0-100)
        (
            -- recent sightings are the strongest signal
            LEAST(recent_sightings_7d * 15, 40)

            -- historical hotspot weight
            + CASE
                WHEN historical_incident_count > 20 THEN 20
                WHEN historical_incident_count > 10 THEN 12
                WHEN historical_incident_count > 5 THEN 6
                ELSE 2
              END

            -- heavy rain drives baitfish inshore
            + CASE
                WHEN avg_rainfall_7d_mm > 50 THEN 15
                WHEN avg_rainfall_7d_mm > 20 THEN 8
                ELSE 0
              END

            -- whale season = more large sharks
            + CASE WHEN is_whale_season THEN 10 ELSE 0 END

            -- dawn and dusk are higher risk
            + CASE
                WHEN current_time_of_day IN ('dawn', 'dusk') THEN 10
                WHEN current_time_of_day = 'night' THEN 5
                ELSE 0
              END
        ) AS risk_score

    FROM risk_factors
)

SELECT
    location_name,
    state,
    score_date,
    risk_score,
    CASE
        WHEN risk_score >= 50 THEN 'High'
        WHEN risk_score >= 25 THEN 'Medium'
        ELSE 'Low'
    END AS risk_level,
    recent_sightings_7d,
    avg_rainfall_7d_mm,
    is_whale_season,
    current_time_of_day AS time_of_day_risk,
    historical_incident_count

FROM scored
ORDER BY risk_score DESC

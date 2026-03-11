{{ config(
    materialized = 'view'
) }}

WITH joined_results AS (

    SELECT
        ra.race_id,
        ra.race_name,
        ra.race_date,
        ra.year AS race_year,
        ra.circuit_name,
        ra.country_name,
        ra.location_city,

        dc.driver_number,
        dc.points,
        dc.position,

        dr.driver_id,
        dr.full_name,
        dr.racing_team,

        te.team_id,
        te.team_name,
        te.team_colour

    FROM {{ ref('silver_races_location') }} ra
    JOIN {{ ref('silver_driver_championship') }} dc
        ON ra.race_id = dc.race_id
    JOIN {{ ref('silver_drivers') }} dr
        ON dc.driver_number = dr.driver_number
    JOIN {{ ref('silver_teams') }} te
        ON dr.racing_team = te.team_name

),

ranked_results AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY race_id, driver_number
            ORDER BY points DESC, position ASC
        ) AS rank_row
    FROM joined_results

)

SELECT
    race_id,
    race_name,
    race_date,
    race_year,
    circuit_name,
    country_name,
    location_city,
    driver_number,
    points,
    position,
    driver_id,
    full_name,
    racing_team,
    team_id,
    team_name,
    team_colour

FROM ranked_results
WHERE rank_row = 1
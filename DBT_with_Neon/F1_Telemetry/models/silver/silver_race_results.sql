SELECT ra.race_id,
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
       INITCAP(dr.full_name) AS full_name,
       dr.racing_team,
       te.team_id,
       te.team_name,
       te.team_colour
FROM {{ ref('silver_races_location') }} ra
JOIN {{ ref('silver_driver_championship') }} dc
    ON ra.race_id=dc.race_id
JOIN {{ ref('silver_drivers') }} dr
    ON dc.driver_number=dr.driver_number
JOIN {{ ref('silver_teams') }} te
    ON dr.racing_team=te.team_name
ORDER BY ra.race_name, dc.position
SELECT full_name,
       racing_team,
       AVG(position) AS avg_finish_position,
       COUNT(*) AS races
FROM {{ ref('silver_race_clean_results') }}
    GROUP BY full_name, racing_team
ORDER BY avg_finish_position
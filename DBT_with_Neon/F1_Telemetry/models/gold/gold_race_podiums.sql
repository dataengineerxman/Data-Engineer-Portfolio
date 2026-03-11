SELECT race_name,
       race_date,
       full_name,
       racing_team,
       position
FROM {{ ref('silver_race_clean_results') }}
WHERE position <=3
ORDER BY race_date, position

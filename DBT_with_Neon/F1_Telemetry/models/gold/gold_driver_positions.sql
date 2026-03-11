SELECT full_name,
       position,
       COUNT(*) AS ocurrences
FROM {{ ref('silver_race_clean_results') }}
    GROUP BY full_name, position
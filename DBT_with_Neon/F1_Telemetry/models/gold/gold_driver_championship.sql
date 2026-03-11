SELECT
    driver_id,
    full_name,
    race_name,
    points,
    SUM(points) OVER (PARTITION BY driver_id) AS total_points
FROM {{ ref('silver_race_clean_results') }}
ORDER BY full_name, total_points DESC
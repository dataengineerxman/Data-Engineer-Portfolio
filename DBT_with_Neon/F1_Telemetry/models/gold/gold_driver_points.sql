SELECT driver_id,
       full_name,
       SUM(points) AS total_points
FROM {{ ref('silver_race_clean_results') }}
    GROUP BY driver_id, full_name
ORDER BY total_points DESC
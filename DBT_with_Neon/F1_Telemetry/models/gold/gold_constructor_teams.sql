SELECT team_name,
       team_colour,
       SUM(points) AS total_points
FROM {{ ref ('silver_team_championship') }}
    GROUP BY team_name, team_colour
ORDER BY total_points DESC
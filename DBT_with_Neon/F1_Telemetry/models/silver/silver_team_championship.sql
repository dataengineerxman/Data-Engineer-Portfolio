SELECT  t.team_id,
        t.team_name,
        t.team_colour,
        tc.points,
        tc.position,
        tc.race_id
FROM {{ ref('silver_teams') }} t
JOIN bronze.team_championship tc
ON tc.team_name=t.team_name
ORDER BY tc.race_id, tc.points DESC
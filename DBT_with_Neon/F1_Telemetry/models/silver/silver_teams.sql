SELECT
    ROW_NUMBER() OVER (ORDER BY team_name) AS team_id,
    team_name,
    team_colour AS team_colour_code,
    CASE team_colour
        WHEN '3671c6' THEN 'Royal Blue'
        WHEN '64c4ff' THEN 'Light Sky Blue'
        WHEN '6692ff' THEN 'Cornflower Blue'
        WHEN 'ff8000' THEN 'Orange'
        WHEN 'ff87bc' THEN 'Pink'
        WHEN '229971' THEN 'Teal Green'
        WHEN 'e8002d' THEN 'Crimson Red'
        WHEN 'b6babd' THEN 'Silver Gray'
        WHEN '52e252' THEN 'Lime Green'
        WHEN '27f4d2' THEN 'Aqua / Turquoise'
    ELSE ''
    END AS team_colour
FROM bronze.teams
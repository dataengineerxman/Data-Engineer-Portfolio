SELECT
    ROW_NUMBER() OVER (ORDER BY driver_number) AS driver_id,
    driver_number,
    initcap(full_name) as full_name,
    name_acronym AS short_name,
    initcap(split_part(full_name, ' ', 1)) AS first_name,
    initcap(split_part(full_name, ' ', 2)) AS last_name,
    team_name AS racing_team
FROM bronze.drivers


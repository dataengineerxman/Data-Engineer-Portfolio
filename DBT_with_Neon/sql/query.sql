SELECT * FROM bronze.drivers;

/*Silver drivers*/
SELECT
        ROW_NUMBER() OVER (ORDER BY driver_number) AS driver_id,
        driver_number,
        full_name,
        initcap(split_part(full_name,' ',1))AS first_name,
        initcap(split_part(full_name,' ',2)) AS last_name,
        team_name AS racing_team
FROM bronze.drivers;


/*Silver driver championship*/
SELECT driver_number,
       points,
       position,
       race_id
FROM bronze.driver_championship
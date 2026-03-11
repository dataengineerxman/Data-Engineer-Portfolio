SELECT
        ROW_NUMBER() OVER () AS race_location_number,
        ra.race_id,
        ra.race_name,
        ra.race_date,
        ra.year,
        ra.circuit_name,
        lo.country_name,
        lo.location_city
FROM bronze.races ra
JOIN bronze.locations lo
ON ra.race_name=lo.circuit_name
-- models/silver/bronze_parking_violation_codes.sql

SELECT
    code AS violation_code,
    definition,
    manhattan_96th_st_below,
    all_other_areas
FROM
    nyc_parking_violations.bronze.violation_codes
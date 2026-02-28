SELECT *,
       CASE
           WHEN violation_county ='BX'
           THEN TRUE
           ELSE FALSE
        END AS is_manhattan_96th_st_below
FROM {{ ref('bronze_parking_violations')}}
SELECT  pvc.violation_code,
        pvc.definition,
        pvc.is_manhattan_96th_st_below,
        pvc.fee_usd,
        pv.summons_number,
        pv.registration_state,
        pv.plate_type,
        pv.issue_date,
        pv.violation_code,
        pv.vehicle_body_type,
        pv.vehicle_make,
        pv.issuing_agency,
        pv.vehicle_expiration_date,
        pv.violation_location,
        pv.violation_precinct,
        pv.issuer_precinct,
        pv.issuer_code,
        pv.issuer_command,
        pv.issuer_squad,
        pv.violation_time,
        pv.violation_county,
        pv.violation_legal_code,
        pv.vehicle_color,
        pv.vehicle_year,
        pv.is_manhattan_96th_st_below
FROM {{ref('silver_parking_violation_codes')}} AS pvc
JOIN {{ref('silver_parking_violations')}} AS pv
ON pvc.violation_code=pv.violation_code
AND pvc.is_manhattan_96th_st_below=pv.is_manhattan_96th_st_below
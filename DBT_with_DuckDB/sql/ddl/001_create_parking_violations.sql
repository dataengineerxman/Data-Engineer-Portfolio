DROP database nyc_parking_violations;

CREATE TABLE IF NOT EXISTS bronze.parking_violations (
    summons_number BIGINT,
    plate_id VARCHAR,
    registration_state VARCHAR,
    plate_type VARCHAR,
    issue_date DATE,
    violation_code INTEGER,
    vehicle_body_type VARCHAR,
    vehicle_make VARCHAR,
    issuing_agency VARCHAR,
    street_code1 INTEGER,
    street_code2 INTEGER,
    street_code3 INTEGER,
    vehicle_expiration_date DATE,
    violation_location VARCHAR,
    violation_precinct INTEGER,
    issuer_precinct INTEGER,
    issuer_code INTEGER,
    issuer_command VARCHAR,
    issuer_squad VARCHAR,
    violation_time VARCHAR,
    violation_county VARCHAR,
    violation_in_front_of_or_opposite VARCHAR,
    house_number VARCHAR,
    street_name VARCHAR,
    intersecting_street VARCHAR,
    date_first_observed DATE,
    law_section INTEGER,
    sub_division VARCHAR,
    violation_legal_code VARCHAR,
    days_parking_in_effect VARCHAR,
    from_hours_in_effect VARCHAR,
    to_hours_in_effect VARCHAR,
    vehicle_color VARCHAR,
    unregistered_vehicle BOOLEAN,
    vehicle_year INTEGER,
    meter_number VARCHAR,
    feet_from_curb INTEGER,
    violation_post_code VARCHAR,
    violation_description VARCHAR,
    no_standing_or_stopping_violation VARCHAR,
    hydrant_violation VARCHAR,
    double_parking_violation VARCHAR
);

ALTER TABLE bronze.parking_violations
ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;

ALTER TABLE bronze.parking_violations
ADD COLUMN IF NOT EXISTS source_file VARCHAR;


DROP TABLE bronze.parking_violations;



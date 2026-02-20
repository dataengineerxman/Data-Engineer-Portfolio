CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.violation_codes (
    violation_code INTEGER,
    definition VARCHAR,
    manhattan_96th_st_below INTEGER,
    manhattan_96th_st_above INTEGER,
    all_other_areas INTEGER
);

ALTER TABLE bronze.violation_codes
ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;

ALTER TABLE bronze.violation_codes
ADD COLUMN IF NOT EXISTS source_file VARCHAR;


DROP TABLE bronze.violation_codes;
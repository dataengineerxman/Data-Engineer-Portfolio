import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "nyc_parking_violations.db"

def initialize_schema():

    print("Connecting to DB:", DB_PATH.resolve())

    conn = duckdb.connect(str(DB_PATH))

    try:
        conn.execute("BEGIN;")

        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        conn.execute("""
            DROP TABLE IF EXISTS bronze.parking_violations;
        """)

        conn.execute("""
            CREATE TABLE bronze.parking_violations (
                summons_number BIGINT,
                plate_id VARCHAR,
                registration_state VARCHAR,
                plate_type VARCHAR,
                issue_date VARCHAR,
                violation_code INTEGER,
                vehicle_body_type VARCHAR,
                vehicle_make VARCHAR,
                issuing_agency VARCHAR,
                street_code1 VARCHAR,
                street_code2 VARCHAR,
                street_code3 VARCHAR,
                vehicle_expiration_date VARCHAR,
                violation_location VARCHAR,
                violation_precinct VARCHAR,
                issuer_precinct VARCHAR,
                issuer_code VARCHAR,
                issuer_command VARCHAR,
                issuer_squad VARCHAR,
                violation_time VARCHAR,
                violation_county VARCHAR,
                violation_in_front_of_or VARCHAR,
                street_name VARCHAR,
                intersecting_street VARCHAR,
                date_first_observed VARCHAR,
                law_section VARCHAR,
                sub_division VARCHAR,
                days_parking_in_effect VARCHAR,
                from_hours_in_effect VARCHAR,
                to_hours_in_effect VARCHAR,
                vehicle_color VARCHAR,
                unregistered_vehicle VARCHAR,
                vehicle_year VARCHAR,
                meter_number VARCHAR,
                feet_from_curb VARCHAR,
                house_number VARCHAR,
                time_first_observed VARCHAR,
                violation_legal_code VARCHAR,
                violation_description VARCHAR,
                ingested_at TIMESTAMP,
                source_file VARCHAR
            );
        """)

        conn.execute("COMMIT;")
        print("Schema ready.")

    except Exception as e:
        conn.execute("ROLLBACK;")
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    initialize_schema()





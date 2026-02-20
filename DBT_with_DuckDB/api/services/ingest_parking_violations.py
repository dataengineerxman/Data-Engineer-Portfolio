import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "nyc_parking_violations.db"
RAW_FILE = BASE_DIR / "data" / "raw" / "parking_violations_20260218_173323.csv"

def load_parking_violations():

    if not RAW_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {RAW_FILE}")

    print("Connecting to DB:", DB_PATH.resolve())
    print("Loading file:", RAW_FILE.resolve())

    conn = duckdb.connect(str(DB_PATH))

    try:
        conn.execute("BEGIN;")

        conn.execute("DELETE FROM bronze.parking_violations;")

        conn.execute(f"""
            INSERT INTO bronze.parking_violations (
                summons_number,
                plate_id,
                registration_state,
                plate_type,
                issue_date,
                violation_code,
                vehicle_body_type,
                vehicle_make,
                issuing_agency,
                street_code1,
                street_code2,
                street_code3,
                vehicle_expiration_date,
                violation_location,
                violation_precinct,
                issuer_precinct,
                issuer_code,
                issuer_command,
                issuer_squad,
                violation_time,
                violation_county,
                violation_in_front_of_or,
                street_name,
                intersecting_street,
                date_first_observed,
                law_section,
                sub_division,
                days_parking_in_effect,
                from_hours_in_effect,
                to_hours_in_effect,
                vehicle_color,
                unregistered_vehicle,
                vehicle_year,
                meter_number,
                feet_from_curb,
                house_number,
                time_first_observed,
                violation_legal_code,
                violation_description,
                ingested_at,
                source_file
            )
            SELECT 
                *,
                CURRENT_TIMESTAMP,
                '{RAW_FILE.name}'
            FROM read_csv_auto(
                '{RAW_FILE}',
                normalize_names=true
            );
        """)

        conn.execute("COMMIT;")

        count = conn.execute(
            "SELECT COUNT(*) FROM bronze.parking_violations;"
        ).fetchone()[0]

        print("Load successful.")
        print("Rows inserted:", count)

    except Exception as e:
        conn.execute("ROLLBACK;")
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    load_parking_violations()




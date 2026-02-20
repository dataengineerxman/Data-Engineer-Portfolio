import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "nyc_parking_violations.db"

def initialize_violation_codes_schema():

    print("Connecting to DB:", DB_PATH.resolve())

    conn = duckdb.connect(str(DB_PATH))

    try:
        conn.execute("BEGIN;")

        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        conn.execute("DROP TABLE IF EXISTS bronze.violation_codes;")

        conn.execute("""
            CREATE TABLE bronze.violation_codes (
                code INTEGER,
                definition VARCHAR,
                manhattan_96th_st_below INTEGER,
                manhattan_96th_st_above INTEGER,
                all_other_areas INTEGER,
                ingested_at TIMESTAMP,
                source_file VARCHAR
            );
        """)

        conn.execute("COMMIT;")
        print("Violation Codes schema ready.")

    except Exception as e:
        conn.execute("ROLLBACK;")
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    initialize_violation_codes_schema()

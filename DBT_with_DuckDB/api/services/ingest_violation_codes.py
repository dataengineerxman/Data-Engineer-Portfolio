import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "nyc_parking_violations.db"
RAW_DIR = BASE_DIR / "data" / "raw"

def get_latest_file():
    files = sorted(RAW_DIR.glob("violation_codes_*.csv"), reverse=True)
    if not files:
        raise FileNotFoundError("No violation_codes_*.csv file found.")
    return files[0]

def load_violation_codes():

    latest_file = get_latest_file()

    print("Connecting to DB:", DB_PATH.resolve())
    print("Loading file:", latest_file.resolve())

    conn = duckdb.connect(str(DB_PATH))

    try:
        conn.execute("BEGIN;")

        # Create or replace table based on actual CSV structure
        conn.execute(f"""
            CREATE OR REPLACE TABLE bronze.violation_codes AS
            SELECT
                *,
                CURRENT_TIMESTAMP AS ingested_at,
                '{latest_file.name}' AS source_file
            FROM read_csv_auto(
                '{latest_file}',
                normalize_names=true
            );
        """)

        conn.execute("COMMIT;")

        count = conn.execute(
            "SELECT COUNT(*) FROM bronze.violation_codes;"
        ).fetchone()[0]

        print("Load successful.")
        print("Rows inserted:", count)

    except Exception as e:
        conn.execute("ROLLBACK;")
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    load_violation_codes()
import duckdb
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Database path
DB_PATH = PROJECT_ROOT / "data" / "nyc_parking_violations.db"

# SQL file path
# SQL_FILE = PROJECT_ROOT / "sql" / "dml" / "query_data.sql"
SQL_FILE = PROJECT_ROOT / "sql" / "dml" / "query_test_data.sql"

# Read SQL query
with open(SQL_FILE, "r", encoding="utf-8") as file:
    query = file.read()

# Connect
con = duckdb.connect(str(DB_PATH))

try:
    result = con.execute(query)

    print("\n===== QUERY RESULTS =====\n")

    # Print column names
    columns = [desc[0] for desc in result.description]
    print(" | ".join(columns))

    # Print first 20 rows safely
    rows = result.fetchmany(20)
    for row in rows:
        print(row)

    print("\nDisplayed first 20 rows only.")

except Exception as e:
    print("\nERROR executing query:")
    print(e)

finally:
    con.close()
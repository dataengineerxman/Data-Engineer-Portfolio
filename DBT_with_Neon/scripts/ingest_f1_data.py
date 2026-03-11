import os
from io import StringIO
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

NEON_CONN_STRING = "postgresql://neondb_owner:npg_7KWYrkuMeOo0@ep-blue-forest-aigce519-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require"

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw_data")

print("Using data directory:", DATA_DIR)

CREATE_TABLES_SQL = """
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.races (
    race_id BIGINT PRIMARY KEY,
    race_name TEXT,
    circuit_name TEXT,
    race_date TIMESTAMP,
    year INT
);

CREATE TABLE IF NOT EXISTS bronze.locations (
    circuit_name TEXT PRIMARY KEY,
    country_name TEXT,
    location_city TEXT
);

CREATE TABLE IF NOT EXISTS bronze.teams (
    team_name TEXT PRIMARY KEY,
    team_colour TEXT
);

CREATE TABLE IF NOT EXISTS bronze.drivers (
    driver_number INT PRIMARY KEY,
    full_name TEXT,
    name_acronym TEXT,
    team_name TEXT
);

CREATE TABLE IF NOT EXISTS bronze.car_traces (
    race_id BIGINT,
    driver_number INT,
    timestamp TIMESTAMP,
    speed FLOAT,
    throttle FLOAT,
    brake FLOAT,
    gear INT,
    drs INT
);

CREATE TABLE IF NOT EXISTS bronze.driver_championship (
    driver_number INT,
    full_name TEXT,
    team_name TEXT,
    points FLOAT,
    position INT,
    race_id BIGINT
);

CREATE TABLE IF NOT EXISTS bronze.team_championship (
    team_name TEXT,
    points FLOAT,
    position INT,
    race_id BIGINT
);
"""

FILES = {
    "races": "races.csv",
    "locations": "locations.csv",
    "teams": "teams.csv",
    "drivers": "drivers.csv",
    "car_traces": "car_traces.csv",
}

PRIMARY_KEYS = {
    "races": "race_id",
    "locations": "circuit_name",
    "teams": "team_name",
    "drivers": "driver_number",
}

def normalize(df):
    df = df.where(pd.notnull(df), None)
    return df.astype(object)

def insert_small_table(conn, table, df):

    pk = PRIMARY_KEYS.get(table)

    if pk and pk in df.columns:
        df = df.drop_duplicates(subset=[pk])
    else:
        df = df.drop_duplicates()

    df = normalize(df)

    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    cols = list(df.columns)

    with conn.cursor() as cur:

        print("Clearing table:", table)
        cur.execute(f"TRUNCATE TABLE bronze.{table}")

        if rows:
            sql = f"""
            INSERT INTO bronze.{table} ({",".join(cols)})
            VALUES %s
            """
            execute_values(cur, sql, rows)

    conn.commit()

def load_car_traces(conn, filepath):

    df = pd.read_csv(filepath)

    if "date" in df.columns:
        df.rename(columns={"date": "timestamp"}, inplace=True)

    if "n_gear" in df.columns:
        df.rename(columns={"n_gear": "gear"}, inplace=True)

    if "timestamp" not in df.columns:
        print("Skipping car_traces — timestamp column missing")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    cols = [
        "race_id",
        "driver_number",
        "timestamp",
        "speed",
        "throttle",
        "brake",
        "gear",
        "drs",
    ]

    df = df[[c for c in cols if c in df.columns]]

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with conn.cursor() as cur:

        print("Clearing table: car_traces")
        cur.execute("TRUNCATE TABLE bronze.car_traces")

        if not df.empty:
            cur.copy_expert(
                """
                COPY bronze.car_traces (
                    race_id,
                    driver_number,
                    timestamp,
                    speed,
                    throttle,
                    brake,
                    gear,
                    drs
                )
                FROM STDIN WITH CSV
                """,
                buffer,
            )

    conn.commit()

def _single_col(df, col_name):
    if col_name not in df.columns:
        return None
    s = df[col_name]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    return s

def load_driver_championship(conn):

    path = os.path.join(DATA_DIR, "driver_championship.csv")

    if not os.path.exists(path):
        print("driver_championship.csv not found")
        return

    df = pd.read_csv(path)

    df = df.rename(columns={
        "points_current": "points",
        "position_current": "position"
    })

    clean = pd.DataFrame()

    clean["driver_number"] = _single_col(df, "driver_number")
    clean["full_name"] = _single_col(df, "full_name")
    clean["team_name"] = _single_col(df, "team_name")
    clean["points"] = _single_col(df, "points")
    clean["position"] = _single_col(df, "position")

    race_series = _single_col(df, "race_id")
    if race_series is None:
        race_series = _single_col(df, "session_key")

    clean["race_id"] = race_series

    clean = clean.drop_duplicates()
    clean = normalize(clean)

    rows = [tuple(r) for r in clean.itertuples(index=False, name=None)]

    with conn.cursor() as cur:

        print("Clearing table: driver_championship")
        cur.execute("TRUNCATE TABLE bronze.driver_championship")

        if rows:
            sql = """
            INSERT INTO bronze.driver_championship
            (driver_number, full_name, team_name, points, position, race_id)
            VALUES %s
            """
            execute_values(cur, sql, rows)

    conn.commit()

def load_team_championship(conn):

    path = os.path.join(DATA_DIR, "team_championship.csv")

    if not os.path.exists(path):
        print("team_championship.csv not found")
        return

    df = pd.read_csv(path)

    df = df.rename(columns={
        "points_current": "points",
        "position_current": "position"
    })

    clean = pd.DataFrame()

    clean["team_name"] = _single_col(df, "team_name")
    clean["points"] = _single_col(df, "points")
    clean["position"] = _single_col(df, "position")

    race_series = _single_col(df, "race_id")
    if race_series is None:
        race_series = _single_col(df, "session_key")

    clean["race_id"] = race_series

    clean = clean.drop_duplicates()
    clean = normalize(clean)

    rows = [tuple(r) for r in clean.itertuples(index=False, name=None)]

    with conn.cursor() as cur:

        print("Clearing table: team_championship")
        cur.execute("TRUNCATE TABLE bronze.team_championship")

        if rows:
            sql = """
            INSERT INTO bronze.team_championship
            (team_name, points, position, race_id)
            VALUES %s
            """
            execute_values(cur, sql, rows)

    conn.commit()

def main():

    print("Connecting to Neon...")
    conn = psycopg2.connect(NEON_CONN_STRING)
    print("Connected")

    with conn.cursor() as cur:

        cur.execute(CREATE_TABLES_SQL)

        # SAFE COLUMN CREATION (prevents your error)
        cur.execute("""
        ALTER TABLE bronze.races
        ADD COLUMN IF NOT EXISTS circuit_name TEXT
        """)

    conn.commit()

    for table, file in FILES.items():

        print("\nLoading", table)

        path = os.path.join(DATA_DIR, file)

        if not os.path.exists(path):
            print("Missing file:", path)
            continue

        if table == "car_traces":
            load_car_traces(conn, path)
        else:
            df = pd.read_csv(path)
            insert_small_table(conn, table, df)

    load_driver_championship(conn)
    load_team_championship(conn)

    conn.close()

if __name__ == "__main__":
    main()
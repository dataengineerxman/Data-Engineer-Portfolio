import os
import sys
import json
import subprocess
import urllib.parse
import pandas as pd

BASE_API = "https://api.openf1.org/v1"
YEAR = 2024
NUM_RACES = 5
DOWNSAMPLE_STEP = 5


def find_project_root(start_dir):
    cur = os.path.abspath(start_dir)
    while True:
        if os.path.isdir(os.path.join(cur, "data")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            raise RuntimeError("Could not find project root containing 'data' folder.")
        cur = parent


def fetch(endpoint, params=None):
    base = f"{BASE_API}/{endpoint}"

    if params:
        query = urllib.parse.urlencode(params)
        url = f"{base}?{query}"
    else:
        url = base

    cmd = ["curl", "-sS", "-L", "--compressed", url]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    if not result.stdout.strip():
        return []

    return json.loads(result.stdout)


def to_dataframe(payload):
    if payload is None:
        return pd.DataFrame()

    if isinstance(payload, list):
        if len(payload) == 0:
            return pd.DataFrame()
        return pd.DataFrame(payload)

    if isinstance(payload, dict):
        if len(payload) == 0:
            return pd.DataFrame()
        return pd.DataFrame([payload])

    return pd.DataFrame()


def save_csv(df, out_dir, filename):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    df.to_csv(path, index=False)
    print(f"WROTE: {filename} ({len(df)} rows)")
    return path


def main():

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = find_project_root(script_dir)
    out_dir = os.path.join(project_root, "data", "raw_data")

    print("PROJECT_ROOT:", project_root)
    print("OUTPUT_DIR:", out_dir)
    print("NUM_RACES:", NUM_RACES)

    sessions = fetch("sessions", {"year": YEAR})
    sessions_df = to_dataframe(sessions)

    if sessions_df.empty:
        raise RuntimeError("No sessions returned from OpenF1.")

    races_df = sessions_df[
        sessions_df["session_name"].astype(str).str.contains("Race", case=False, na=False)
    ].copy()

    if races_df.empty:
        raise RuntimeError("No race sessions found.")

    races_df = races_df.sort_values("date_start").head(NUM_RACES)

    all_races = []
    all_locations = []
    all_teams = []
    all_drivers = []
    all_traces = []
    all_driver_champ = []
    all_team_champ = []

    for _, race in races_df.iterrows():

        race_id = int(race["session_key"])
        meeting_key = int(race["meeting_key"])

        race_start = pd.to_datetime(race["date_start"], errors="coerce")
        race_end = pd.to_datetime(race["date_end"], errors="coerce")

        print(f"\nProcessing race_id={race_id}")

        all_races.append({
            "race_id": race_id,
            "race_name": race.get("circuit_short_name", ""),
            "circuit_name": race.get("circuit_short_name", ""),   # ← ONLY ADDITION
            "race_date": race.get("date_start", ""),
            "year": int(race.get("year", YEAR))
        })

        all_locations.append({
            "circuit_name": race.get("circuit_short_name", ""),
            "country_name": race.get("country_name", ""),
            "location_city": race.get("location", "")
        })

        drivers = fetch("drivers", {"session_key": race_id})
        drivers_df = to_dataframe(drivers)

        if drivers_df.empty:
            print(f"No drivers returned for race_id={race_id}")
            continue

        driver_cols = [c for c in ["driver_number","full_name","name_acronym","team_name"] if c in drivers_df.columns]
        team_cols = [c for c in ["team_name","team_colour"] if c in drivers_df.columns]

        if team_cols:
            all_teams.append(drivers_df[team_cols].drop_duplicates())

        if driver_cols:
            all_drivers.append(drivers_df[driver_cols].drop_duplicates())

        car_data = fetch("car_data", {"session_key": race_id})
        ddf = to_dataframe(car_data)

        if not ddf.empty:

            required_cols = {"driver_number","date","speed","throttle","brake","n_gear","drs"}
            available = required_cols.intersection(ddf.columns)

            if "date" in available:
                ddf["date"] = pd.to_datetime(ddf["date"], errors="coerce")
                ddf = ddf.dropna(subset=["date"])
                ddf = ddf[(ddf["date"] >= race_start) & (ddf["date"] <= race_end)]

            ddf = ddf.iloc[::DOWNSAMPLE_STEP].copy()
            ddf["race_id"] = race_id

            needed_cols = ["race_id","driver_number","date","speed","throttle","brake","n_gear","drs"]
            needed_cols = [c for c in needed_cols if c in ddf.columns]

            if needed_cols:
                all_traces.append(ddf[needed_cols])

        driver_champ = fetch("championship_drivers", {"meeting_key": meeting_key})
        driver_champ_df = to_dataframe(driver_champ)

        if not driver_champ_df.empty:

            driver_champ_df["race_id"] = race_id

            cols = [
                "driver_number",
                "full_name",
                "team_name",
                "points_current",
                "position_current",
                "race_id"
            ]

            cols = [c for c in cols if c in driver_champ_df.columns]

            all_driver_champ.append(driver_champ_df[cols])

        team_champ = fetch("championship_teams", {"meeting_key": meeting_key})
        team_champ_df = to_dataframe(team_champ)

        if not team_champ_df.empty:

            team_champ_df["race_id"] = race_id

            cols = [
                "team_name",
                "points_current",
                "position_current",
                "race_id"
            ]

            cols = [c for c in cols if c in team_champ_df.columns]

            all_team_champ.append(team_champ_df[cols])

    save_csv(pd.DataFrame(all_races).drop_duplicates(), out_dir, "races.csv")
    save_csv(pd.DataFrame(all_locations).drop_duplicates(), out_dir, "locations.csv")

    if all_teams:
        save_csv(pd.concat(all_teams, ignore_index=True).drop_duplicates(), out_dir, "teams.csv")

    if all_drivers:
        save_csv(pd.concat(all_drivers, ignore_index=True).drop_duplicates(), out_dir, "drivers.csv")

    if all_traces:

        traces_df = pd.concat(all_traces, ignore_index=True).drop_duplicates()

        final_cols = ["race_id","driver_number","date","speed","throttle","brake","n_gear","drs"]
        final_cols = [c for c in final_cols if c in traces_df.columns]

        traces_df = traces_df[final_cols].copy()

        rename_map = {}
        if "date" in traces_df.columns:
            rename_map["date"] = "timestamp"

        if "n_gear" in traces_df.columns:
            rename_map["n_gear"] = "gear"

        traces_df.rename(columns=rename_map, inplace=True)

        save_csv(traces_df, out_dir, "car_traces.csv")

    if all_driver_champ:
        save_csv(pd.concat(all_driver_champ, ignore_index=True).drop_duplicates(), out_dir, "driver_championship.csv")

    if all_team_champ:
        save_csv(pd.concat(all_team_champ, ignore_index=True).drop_duplicates(), out_dir, "team_championship.csv")

    print("\nDONE.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", str(e))
        sys.exit(1)
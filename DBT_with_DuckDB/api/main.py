from fastapi import FastAPI, Query
import requests

app = FastAPI()

# ----- Dataset 1: Violation Codes -----
VIOLATION_CODES_ID = "ncbg-6agr"
VIOLATION_CODES_URL = f"https://data.cityofnewyork.us/resource/{VIOLATION_CODES_ID}.json"

@app.get("/violation-codes")
def get_violation_codes(limit: int = Query(10, le=1000)):
    params = {"$limit": limit}

    response = requests.get(VIOLATION_CODES_URL, params=params)
    response.raise_for_status()
    return response.json()


# ----- Dataset 2: Parking Violations FY2023 -----
PARKING_VIOLATIONS_ID = "869v-vr48"
PARKING_VIOLATIONS_URL = f"https://data.cityofnewyork.us/resource/{PARKING_VIOLATIONS_ID}.json"

@app.get("/parking-violations")
def get_parking_violations(limit: int = Query(10, le=1000)):
    params = {"$limit": limit}

    response = requests.get(PARKING_VIOLATIONS_URL, params=params)
    response.raise_for_status()
    return response.json()


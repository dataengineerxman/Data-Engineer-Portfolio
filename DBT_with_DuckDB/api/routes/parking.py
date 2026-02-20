from Tools.scripts.freeze_modules import resolve_modules
from fastapi import APIRouter, Query
import requests

router = APIRouter()

DATASET_ID='ncbg-6agr'
BASE_URL=f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"

@router.get("/violation-coded")
def get_violation_codes(limit: = Query(10,le=1000)):
    params={"limit":limit}
    response=requests.get(BASE_URL,params=params)
    response.raise_for_status()
    return response.json()

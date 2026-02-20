from pathlib import Path

NYC_API_BASE = "https://data.cityofnewyork.us/resource"

PARKING_VIOLATIONS_ID = "869v-vr48"
VIOLATION_CODES_ID = "ncbg-6agr"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

DATA_DIR.mkdir(exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

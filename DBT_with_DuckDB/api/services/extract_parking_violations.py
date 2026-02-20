# Imports
import requests
import pandas as pd
from datetime import datetime
from api.config import NYC_API_BASE, PARKING_VIOLATIONS_ID, RAW_DIR

BASE_URL = f"{NYC_API_BASE}/{PARKING_VIOLATIONS_ID}.json"


def download_parking_violations(limit: int = 10000):
    params = {"$limit": limit}

    print("Calling:", BASE_URL)

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_DIR / f"parking_violations_{timestamp}.csv"

    print("Saving to:", file_path.resolve())

    df.to_csv(file_path, index=False)

    print(f"Saved file to {file_path}")
    print(f"Rows saved: {len(df)}")

    return file_path


if __name__ == "__main__":
    download_parking_violations()


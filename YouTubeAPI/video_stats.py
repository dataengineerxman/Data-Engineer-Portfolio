import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

CHANNEL_HANDLE="MrBeast"
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
url = "https://youtube.googleapis.com/youtube/v3/channels"

params = {
    "part": "contentDetails",
    "forHandle": CHANNEL_HANDLE,
    "key": YOUTUBE_API_KEY
}

response = requests.get(url, params=params)
response.raise_for_status()

data = response.json()
print(json.dumps(data, indent=4))



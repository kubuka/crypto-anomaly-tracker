from requests import get
import json
import os
from dotenv import load_dotenv

load_dotenv()


def pull():
    api_key = os.getenv("API_KEY")

    if not api_key:
        raise ValueError("API_KEY environment variable not set")

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
    }

    url = f"https://api.coingecko.com/api/v3/coins/markets?x_cg_demo_api_key={api_key}"

    allcoins = []
    pages = 2
    while pages > 0:
        response = get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            allcoins.extend(data)
            pages -= 1
            params["page"] += 1
        else:
            print(f"API request failed with status code: {response.status_code}")
            break

    with open("/opt/data/bronze/coins.json", "w", encoding="utf-8") as f:
        print(f"Saving {len(allcoins)} coins to coins.json")
        json.dump(allcoins, f, indent=4)

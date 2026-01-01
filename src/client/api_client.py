import requests
from datetime import datetime, timezone
from src.utils.logger import logger
from src.utils.constants import NASA_API_KEY, BASE_URL

def fetch_neo_data():
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    params = {
        "api_key": NASA_API_KEY,
        "start_date": current_date
    }

    response = requests.get(BASE_URL, params=params)

    try:
        return response.json()
    except Exception as e:
        logger.error(f"Error parsing JSON response: {e}")
        return None

fetch_neo_data()
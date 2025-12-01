import json
import requests
from datetime import datetime
from typing_extensions import Optional
from config.logger import setup_logger
from config.constants import API_KEY, API_URL

logger = setup_logger()

def extract_nasa_data(
  start_date: Optional[datetime] = None, 
  end_date: Optional[datetime] = None
):
  params = {}
  if start_date:
    params["start_date"] = start_date
  if end_date:
    params["end_date"] = end_date

  try:
    logger.info("Fetching data from NASA NeoWS...")
    logger.debug(f"With parameters -> {', '.join([f'{key}: {params[key]}' for key in params.keys()])}")

    params["api_key"] = API_KEY
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Successfully fetched data!")
    return data
  except requests.exceptions.HTTPError as e:
    logger.error(f"Error in fetching data: {e}")
    raise
import json, boto3, requests
from typing_extensions import Optional
from config.logger import logger
from config.constants import (
  API_KEY, API_URL, 
  AWS_ACCESS_KEY, AWS_SECRET_KEY, 
  AWS_REGION, AWS_BUCKET_NAME
)
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

def extract_data_and_load_to_aws():
  last_week = (datetime.now() - timedelta(weeks=1)).strftime('%Y-%m-%d')

  data = _extract_nasa_data(last_week)
  _load_to_aws(data)

def _extract_nasa_data(
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

def _load_to_aws(data: dict):
  if not data:
    logger.warning("No extracted data to load")
    return None
  
  try:
    s3 = boto3.client("s3",
      aws_access_key_id=AWS_ACCESS_KEY,
      aws_secret_access_key=AWS_SECRET_KEY,
      region_name=AWS_REGION
    )

    timestamp = int(datetime.now(timezone.utc).timestamp())
    filename = f"neo_{timestamp}.json"
    s3_key = f"raw-neo/{filename}"

    data["status"] = "raw"
    data["ingestion_timestamp"] = timestamp

    json_data = json.dumps(data)
    s3_uri = f"s3://{AWS_BUCKET_NAME}/{s3_key}"
    logger.warning(f"Uploading data to {s3_uri}")

    s3.put_object(
      Bucket=AWS_BUCKET_NAME,
      Key=s3_key,
      Body=json_data.encode("utf-8"),
      ContentType="application/json"
    )

    logger.info(f"Successfully uploaded data to AWS S3: {s3_uri}")
  except ClientError as e:
    logger.error(f"Error uploading data to AWS S3: {e}")
    raise
  except Exception as e:
    logger.error(f"Error connecting to AWS: {e}")
    raise
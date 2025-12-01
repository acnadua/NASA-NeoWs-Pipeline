import boto3
from config.constants import (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, 
    AWS_REGION
)
from src.transform.aws_helper import get_raw_json_from_aws, mark_as_processed
from src.transform.transform_pipeline import transform_raw_data
from config.logger import logger

def transform_raw_data_from_aws():
    s3 = boto3.client("s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    try:
        raw_data = get_raw_json_from_aws(s3)
        for item in raw_data:
            status = transform_raw_data(item)
            mark_as_processed(s3, item["key"], status)
    except Exception as e:
        logger.error(f"Error getting raw data from AWS: {e}")
        raise
    finally:
        s3.close()
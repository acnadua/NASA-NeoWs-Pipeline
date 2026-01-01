import boto3, json
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from src.utils.logger import logger
from src.utils.constants import (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, AWS_BUCKET_NAME
)

class AWSClient:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

    def save_data_to_s3(self, data: dict | None):
        if not data:
            logger.warning("No data to save to S3.")
            return

        try:
            collection_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            filename = f"{collection_date}.json"
            s3_key = f"raw-neo/{filename}"

            data["status"] = "raw"
            data["ingestion_date"] = collection_date

            s3_uri = f"s3://{AWS_BUCKET_NAME}/{s3_key}"
            logger.warning(f"Uploading data to {s3_uri}")

            self.client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType="application/json"
            )

            logger.info(f"Successfully uploaded data to AWS S3: {s3_uri}")
        except ClientError as e:
            logger.error(f"Error uploading data to AWS S3: {e}")
            raise e
        except Exception as e:
            logger.error(f"Error connecting to AWS: {e}")
            raise e
import boto3, json
from datetime import datetime, timezone
from config.logger import logger
from config.constants import AWS_BUCKET_NAME

def get_raw_json_from_aws(s3: boto3.client):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=AWS_BUCKET_NAME,
        Prefix="raw-neo/"
    )

    unprocessed_data = []
    for page in pages:
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            s3_key = obj["Key"]

            if s3_key.endswith("/") or not s3_key.endswith(".json"):
                continue

            processed_data = _get_processing_status(s3, s3_key)
            status = processed_data.get("status", "unknown")
            # if status != "completed":
            unprocessed_data.append({
                "key": s3_key,
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
                "status": status,
                "body": _get_json_data(s3, s3_key)
            })

    logger.info(f"Found {len(unprocessed_data)} unprocessed data")
    return unprocessed_data

def _get_json_data(s3_client: boto3.client, s3_key: str):
    response = s3_client.get_object(
        Bucket=AWS_BUCKET_NAME,
        Key=s3_key
    )
    return json.loads(response["Body"].read().decode("utf-8"))

def mark_as_processed(s3_client: boto3.client, s3_key: str, status: str, error_message: str = None):
    status_key = s3_key \
        .replace("raw-neo/", "processed-neo/") \
        .replace(".json", "_status.json")

    data = {
        "raw_file": s3_key,
        "status": status,
        "processed_at": datetime.now(timezone.utc).isoformat()
    }

    if error_message:
        data["error_message"] = error_message

    s3_client.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=status_key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

    logger.info(f"Marked {s3_key} as {status} with error message: {error_message}")

def _get_processing_status(s3_client: boto3.client, s3_key: str):
    status_key = s3_key \
        .replace("raw-neo/", "processed-neo/") \
        .replace(".json", "_status.json")

    try:
        response = s3_client.get_object(
            Bucket=AWS_BUCKET_NAME,
            Key=status_key
        )

        data = json.loads(response["Body"].read().decode("utf-8"))
        return data
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"No processing status found for {s3_key}")
        return {"status": "unknown"}
    except Exception as e:
        logger.warning(f"Error getting processing status for {s3_key}: {e}")
        return {"status": "unknown"}
import boto3
from config.constants import (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, 
    AWS_REGION
)
from config.logger import logger
from src.transform.aws_access import get_raw_json_from_aws, mark_as_processed
from src.models import neo_json_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, explode, 
    regexp_extract, lit, trim, 
    when, abs, hash
)


def transform_raw_data_from_aws():
    s3 = boto3.client("s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    try:
        raw_data = get_raw_json_from_aws(s3)
        for item in raw_data:
            status = _transform_raw_data(item)
            mark_as_processed(s3, item["key"], status)
    except Exception as e:
        logger.error(f"Error getting raw data from AWS: {e}")
        raise
    finally:
        s3.close()

def _transform_raw_data(raw_data: dict):  
    if not raw_data:
        logger.warning("No raw data to transform")
        return None

    json_data = raw_data.get("body", {})
    near_earth_objects = json_data.get("near_earth_objects", {})
    asteroids = []

    for date in near_earth_objects.keys():
        asteroids.extend(near_earth_objects[date])
  
    new_asteroids = []
    for asteroid in asteroids:
        new_asteroid = asteroid.copy()
        close_approaches = new_asteroid.pop("close_approach_data", [])
        approach_data = {}
        for approach in close_approaches:
            new_approach = approach.copy()
            date = new_approach.pop("close_approach_date_full", "")
            approach_data[date] = new_approach
    
        new_asteroid["close_approach_data"] = approach_data
        new_asteroids.append(new_asteroid)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("NASA_NeoWS_Pipeline") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.extraJavaOptions", "-Xlog:disable") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    status = "unknown"
  
    try:
        logger.info("Created Spark Session. Starting data transformation...")
        raw_df = spark.createDataFrame(new_asteroids, schema=neo_json_schema)
        ingestion_timestamp = raw_data.get("ingestion_timestamp", current_timestamp().cast("long"))

        # 1. create custom history id based on neo_reference_id and ingestion timestamp
        raw_df = raw_df.withColumn(
            "history_id",
            abs(hash(col("neo_reference_id"), ingestion_timestamp))
        )

        logger.info(f"Raw data loaded: {raw_df.count()} celestial objects found")

        # 2. create base dataframe for the NEO
        celestial_object_df = raw_df.select(
            col("neo_reference_id").alias("object_id"),
            col("name"),
            col("nasa_jpl_url"),
            col("is_sentry_object").alias("is_sentry"),
            col("history_id").alias("current_measurements"),
            col("is_potentially_hazardous_asteroid").alias("current_hazard_status")
        )

        # 3. clean celestial object name to get only the actual name
        celestial_object_df = celestial_object_df.withColumn(
            "name", 
            when(
                col("name").rlike(r"\([^)]+\)"),  # If contains parentheses
                trim(regexp_extract(col("name"), r"\(([^)]+)\)", 1))
            ).otherwise(
                trim(col("name"))  # Keep original if no parentheses
            )
        )

        # 4. extract the current measurements of the NEO
        history_df = raw_df.select(
            col("history_id"),
            col("neo_reference_id").alias("object_id"),
            col("estimated_diameter.kilometers.estimated_diameter_min").alias("min_diameter_km"),
            col("estimated_diameter.kilometers.estimated_diameter_max").alias("max_diameter_km"),
            col("absolute_magnitude_h").alias("abs_magnitude_h"),
            ingestion_timestamp.alias("valid_from_date"),
            lit(None).cast("long").alias("valid_to_date"),
            lit(True).cast("boolean").alias("is_current")
        )

        # 5. create close approach table
        close_approach_df = raw_df.select(
            col("neo_reference_id").alias("object_id"),
            explode(col("close_approach_data")).alias("approach_date", "approach_data")
        ).select(
            abs(hash(col("object_id"), col("approach_date"), ingestion_timestamp)).alias("approach_id"),
            col("object_id"),
            col("approach_date").alias("full_date"),
            col("approach_data.epoch_date_close_approach").cast("long").alias("epoch_date"),
            col("approach_data.relative_velocity.kilometers_per_second").alias("relative_velocity_kms"),
            col("approach_data.miss_distance.kilometers").alias("miss_distance_km"),
            col("approach_data.miss_distance.astronomical").alias("miss_distance_au"),
            col("approach_data.orbiting_body").alias("orbiting_body_id")
        )

        # TODO:
        # 6. create celestial body table
    
        status = "completed"
    except Exception as e:
        logger.error(f"Error in PySpark transformation: {e}")
        status = "failed"
    finally:
        spark.stop()
        logger.warning("Spark session stopped")
        return status
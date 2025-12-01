from config.logger import setup_logger
from pyspark.sql import SparkSession
from src.models.asteroid import asteroid_schema

logger = setup_logger()

def load_to_aws(json: dict):
  if not json:
    logger.warning("No extracted data to transform")
    return None
  
  near_earth_objects = json.get("near_earth_objects", {})
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
      .master("local[*]") \
      .config("spark.driver.memory", "2g") \
      .getOrCreate()
  
  try:
    logger.info("Created Spark Session. Starting data transformation...")
    df = spark.createDataFrame(new_asteroids, schema=asteroid_schema)
    logger.debug(df)
  except Exception as e:
    logger.error(f"Error in PySpark transformation: {e}")
    raise
  finally:
    spark.stop()
    logger.warning("Spark session stopped")
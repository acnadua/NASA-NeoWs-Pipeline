from config.logger import setup_logger
from pyspark.sql import SparkSession
from src.models.asteroid import asteroid_schema

logger = setup_logger()
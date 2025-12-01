from pyspark.sql.types import *


celestial_object = StructType([
  StructField("object_id", IntegerType(), nullable=False),
  StructField("name", StringType(), nullable=False),
  StructField("nasa_jpl_url", StringType(), nullable=False),
  StructField("is_sentry", BooleanType(), nullable=False),
  StructField("current_measurements", IntegerType(), nullable=False),
  StructField("current_hazard_status", IntegerType(), nullable=False)
])
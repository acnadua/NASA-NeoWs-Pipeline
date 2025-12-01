from pyspark.sql.types import *

estimated_diameter = StructType([
  StructField("estimated_diameter_min", FloatType()),
  StructField("estimated_diameter_max", FloatType())
])

approach_data = StructType([
  StructField("close_approach_date", StringType()),
  StructField("epoch_date_close_approach", LongType()),
  StructField("relative_velocity", MapType(StringType(), StringType())),
  StructField("miss_distance", MapType(StringType(), StringType())),
  StructField("orbiting_body", StringType())
])

asteroid_schema = StructType([
  StructField("links", MapType(StringType(), StringType())),
  StructField("id", StringType()),
  StructField("neo_reference_id", StringType()),
  StructField("name", StringType()),
  StructField("nasa_jpl_url", StringType()),
  StructField("absolute_magnitude_h", FloatType()),
  StructField("estimated_diameter", MapType(StringType(), estimated_diameter)),
  StructField("is_potentially_hazardous_asteroid", BooleanType()),
  StructField("close_approach_data", MapType(StringType(), approach_data)),
  StructField("is_sentry_object", BooleanType())
])
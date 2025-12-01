from pyspark.sql.types import *

close_approach = StructType([
    StructField("approach_id", IntegerType(), nullable=False),
    StructField("object_id", IntegerType(), nullable=False),
    StructField("orbiting_body_id", StringType(), nullable=False),
    StructField("epoch_date", LongType(), nullable=False),
    StructField("full_date", DateType(), nullable=False),
    StructField("relative_velocity_kms", FloatType(), nullable=False),
    StructField("miss_distance_km", FloatType(), nullable=False),
    StructField("miss_distance_au", FloatType(), nullable=False),
])
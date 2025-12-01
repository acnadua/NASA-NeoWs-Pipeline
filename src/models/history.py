from pyspark.sql.types import *

history = StructType([
    StructField("history_id", IntegerType(), nullable=False),
    StructField("object_id", IntegerType(), nullable=False),
    StructField("min_diameter_km", FloatType(), nullable=False),
    StructField("max_diameter_km", FloatType(), nullable=False),
    StructField("abs_magnitude_h", FloatType(), nullable=False),
    StructField("valid_from_date", LongType(), nullable=False),
    StructField("valid_to_date", LongType()),
    StructField("is_current", BooleanType(), nullable=False)
])
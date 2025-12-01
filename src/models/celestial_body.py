from pyspark.sql.types import *

celestial_body = StructType([
    StructField("body_id", IntegerType(), nullable=False),
    StructField("body_name", StringType(), nullable=False),
    StructField("body_type", StringType(), nullable=False),
])
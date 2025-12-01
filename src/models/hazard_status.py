from pyspark.sql.types import *

hazard_status = StructType([
    StructField("hazard_id", IntegerType(), nullable=False),
    StructField("status", StringType(), nullable=False)
])
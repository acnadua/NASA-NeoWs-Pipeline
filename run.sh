#!/bin/bash
export SPARK_LOCAL_IP=192.168.18.19
export PYSPARK_SUBMIT_ARGS="--conf spark.ui.showConsoleProgress=false pyspark-shell"
export PYTHONWARNINGS="ignore"
python __main__.py 2>&1 | grep -v -E "(WARNING:|Using Spark's|Setting default log level|To adjust logging level|WARN |For SparkR)"
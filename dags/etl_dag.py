from airflow.decorators import dag, task
from datetime import datetime, timedelta
from config.airflow_default_args import default_args
from src.extract.neo_api_client import extract_data_and_load_to_aws

@dag(
  dag_id="nasa_neo_pipeline",
  schedule="0 0 * * 1", # every Monday at midnight
  start_date=datetime(2025, 11, 26),
  dagrun_timeout=timedelta(minutes=5),
  max_active_runs=1,
  catchup=False,
  default_args=default_args,
  description="Runs an ETL pipeline for collecting near Earth objects data from NASA",
  tags=["nasa", "etl", "neo"]
)
def neo_pipeline():
  @task
  def extract_data():
    extract_data_and_load_to_aws()
  
  @task
  def transform_data():
    return transform_nasa_data()
  
  @task
  def load_data():
    return load_nasa_data()
  
  extract_data()
  transform_data()
  load_data()

  log_summary(loaded_data)
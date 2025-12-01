from src.extract.neo_api_client import extract_nasa_data
from src.load.load_to_aws import load_to_aws
from datetime import datetime, timedelta

# extract data
last_week = (datetime.now() - timedelta(weeks=1)).strftime('%Y-%m-%d')
data = extract_nasa_data(last_week)
cleaned_data = load_to_aws(data)
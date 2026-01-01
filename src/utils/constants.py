from dotenv import load_dotenv
import os

load_dotenv()

NASA_API_KEY = os.getenv("NASA_API_KEY", "")

DB_NAME = os.getenv("DB_NAME", "nasa_neows_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "nasa-neows-data-bucket")

BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"
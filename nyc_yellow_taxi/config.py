from dotenv import load_dotenv
import os
import yaml

load_dotenv()

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
PIPELINE_START_DATE = config["pipeline"]["start_date"]
PIPELINE_END_DATE = config["pipeline"]["end_date"]
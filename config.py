from dotenv import load_dotenv
import os

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
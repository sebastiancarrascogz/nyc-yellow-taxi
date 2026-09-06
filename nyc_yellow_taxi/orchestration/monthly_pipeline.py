import datetime
import os
import json
import subprocess
from pathlib import Path
from typing import cast
from dateutil.relativedelta import relativedelta
from prefect import flow, task
from prefect.blocks.system import Secret

from nyc_yellow_taxi.ingestion.load_yellow_trips import ingest_yellow_trips


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "nyc_yellow_taxi"
GCP_CREDENTIALS_PATH = Path("/tmp/gcp-service-account.json")

def configure_gcp_credentials() -> None:
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return

    credentials = cast(Secret, Secret.load("gcp-nyc-taxi-service-account")).get()

    GCP_CREDENTIALS_PATH.write_text(json.dumps(credentials),encoding="utf-8")
    GCP_CREDENTIALS_PATH.chmod(0o600)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
        GCP_CREDENTIALS_PATH
    )

def get_target_batch_month() -> str:
    today = datetime.date.today()
    current_month = today.replace(day=1)
    target_month = current_month - relativedelta(months=4)

    return target_month.isoformat()


@task(retries=2, retry_delay_seconds=60)
def ingest_month(batch_month: str) -> None:
    month = datetime.datetime.strptime(batch_month, "%Y-%m-%d")
    ingest_yellow_trips(start_date=month, end_date=month)


@task
def run_dbt_transformations(batch_month: str) -> None:
    command = [
        "dbt",
        "run",
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--select",
        "yellow_trips+",
        "--vars",
        f"batch_month: {batch_month}",
    ]

    subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        check=True,
    )


@task
def run_dbt_tests() -> None:
    command = [
        "dbt",
        "test",
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--select",
        "yellow_trips+",
    ]

    subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        check=True,
    )


@task
def cleanup_old_data() -> None:
    command = [
        "dbt",
        "run-operation",
        "cleanup_retention",
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
    ]

    subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        check=True,
    )


@flow
def monthly_taxi_pipeline(batch_month: str | None = None) -> None:
    configure_gcp_credentials()

    batch_month = batch_month or get_target_batch_month()

    ingest_month(batch_month)
    run_dbt_transformations(batch_month)
    run_dbt_tests()
    cleanup_old_data()

if __name__ == "__main__":
    monthly_taxi_pipeline()
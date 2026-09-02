import datetime
import subprocess
from pathlib import Path

from dateutil.relativedelta import relativedelta
from prefect import flow, task

from nyc_yellow_taxi.ingestion.load_yellow_trips import ingest_yellow_trips


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "nyc_yellow_taxi"


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
        "--select",
        "yellow_trips+",
    ]

    subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        check=True,
    )


@flow
def monthly_taxi_pipeline(batch_month: str | None = None) -> None:
    batch_month = batch_month or get_target_batch_month()

    ingest_month(batch_month)
    run_dbt_transformations(batch_month)
    run_dbt_tests()

if __name__ == "__main__":
    monthly_taxi_pipeline()
from prefect import flow
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron
from nyc_yellow_taxi.config import settings


SOURCE = GitRepository(
    url="https://github.com/sebastiancarrascogz/nyc-yellow-taxi.git",
    branch="main",
    credentials={
        "access_token": Secret.load("github-nyc-taxi-read")
    },
)

MONTHLY_ENTRYPOINT = (
    "nyc_yellow_taxi/orchestration/"
    "monthly_pipeline.py:monthly_taxi_pipeline"
)

BACKFILL_ENTRYPOINT = (
    "nyc_yellow_taxi/orchestration/"
    "backfill_pipeline.py:backfill_taxi_pipeline"
)

JOB_VARIABLES = {
    "pip_packages": [
        "dlt[bigquery]>=1.29.0,<2.0.0",
        "dbt-core>=1.11.11,<2.0.0",
        "dbt-bigquery>=1.12.0,<2.0.0",
        "python-dotenv>=1.2.2,<2.0.0",
        "pyyaml>=6.0.3,<7.0.0",
        "pandas>=3.0.3,<4.0.0",
        "pyarrow>=25.0.0,<26.0.0",
        "python-dateutil>=2.9.0,<3.0.0",
    ],
    "env": {
        "BIGQUERY_PROJECT_ID": settings.bigquery_project_id,
    },
}

MONTHLY_SCHEDULE = Cron(
    "0 6 5 * *",
    timezone="America/Santiago",
)


if __name__ == "__main__":
    flow.from_source(
        source=SOURCE,
        entrypoint=MONTHLY_ENTRYPOINT,
    ).deploy( # pyright: ignore[reportAttributeAccessIssue]
        name="monthly-taxi-prod",
        work_pool_name="nyc-taxi-serverless",
        job_variables=JOB_VARIABLES,
        schedules=[MONTHLY_SCHEDULE],
    )

    flow.from_source(
        source=SOURCE,
        entrypoint=BACKFILL_ENTRYPOINT,
    ).deploy( # pyright: ignore[reportAttributeAccessIssue]
        name="backfill-taxi-manual",
        work_pool_name="nyc-taxi-serverless",
        job_variables=JOB_VARIABLES,
    )
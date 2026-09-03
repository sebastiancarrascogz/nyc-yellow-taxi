from prefect import flow
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from nyc_yellow_taxi.config import settings

SOURCE = GitRepository(
    url="https://github.com/sebastiancarrascogz/nyc-yellow-taxi.git",
    branch="main",
    credentials={
        "access_token": Secret.load("github-nyc-taxi-read")
    },
)

ENTRYPOINT = (
    "nyc_yellow_taxi/orchestration/"
    "monthly_pipeline.py:monthly_taxi_pipeline"
)


if __name__ == "__main__":
    flow.from_source(
        source=SOURCE,
        entrypoint=ENTRYPOINT,
    ).deploy(  # pyright: ignore[reportAttributeAccessIssue]
        name="monthly-taxi-prod",
        work_pool_name="nyc-taxi-serverless",
        job_variables={
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
        },
    )
from prefect import flow
from prefect.runner.storage import GitRepository

SOURCE = GitRepository(
    url="https://github.com/sebastiancarrascogz/nyc-yellow-taxi.git",
    branch="main",
    pull_interval=60,
)

ENTRYPOINT = (
    "nyc_yellow_taxi/orchestration/"
    "monthly_pipeline.py:monthly_taxi_pipeline"
)

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE,
        entrypoint=ENTRYPOINT,
    ).serve( # pyright: ignore[reportAttributeAccessIssue]
        name="monthly-taxi-prod",
        pause_on_shutdown=False,
    )
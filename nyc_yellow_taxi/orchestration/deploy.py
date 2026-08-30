from pathlib import Path

from nyc_yellow_taxi.orchestration.monthly_pipeline import monthly_taxi_pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[2]


if __name__ == "__main__":
    monthly_taxi_pipeline.from_source(
        source=str(PROJECT_ROOT),
        entrypoint=(
            "nyc_yellow_taxi/orchestration/"
            "monthly_pipeline.py:monthly_taxi_pipeline"
        ),
    ).deploy(  # pyright: ignore[reportAttributeAccessIssue]
        name="monthly-taxi-local",
        work_pool_name="local-process-pool",
    )
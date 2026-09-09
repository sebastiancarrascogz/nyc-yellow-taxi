import datetime

from dateutil.relativedelta import relativedelta
from prefect import flow, get_run_logger

from nyc_yellow_taxi.orchestration.monthly_pipeline import monthly_taxi_pipeline


@flow
def backfill_taxi_pipeline(
    start_month: str,
    end_month: str,
) -> None:
    logger = get_run_logger()

    current_month = datetime.datetime.strptime(
        start_month,
        "%Y-%m-%d",
    ).date()

    final_month = datetime.datetime.strptime(
        end_month,
        "%Y-%m-%d",
    ).date()

    while current_month <= final_month:
        batch_month = current_month.isoformat()

        logger.info("Processing backfill batch: %s", batch_month)

        monthly_taxi_pipeline(
            batch_month=batch_month,
            retention_mode="backfill",
        )

        current_month += relativedelta(months=1)
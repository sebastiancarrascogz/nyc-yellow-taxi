import datetime

from dateutil.relativedelta import relativedelta
from prefect import flow, get_run_logger

from nyc_yellow_taxi.orchestration.monthly_pipeline import (
    monthly_taxi_pipeline,
    cleanup_old_data
)


def parse_batch_month(value: str) -> datetime.date:
    month = datetime.date.fromisoformat(value)

    if month.day != 1:
        raise ValueError(
            f"El batch debe comenzar el día 1: {value}"
        )

    return month


@flow
def backfill_taxi_pipeline(
    start_month: str,
    end_month: str,
) -> None:
    logger = get_run_logger()

    current_month = parse_batch_month(start_month)
    final_month = parse_batch_month(end_month)

    if current_month > final_month:
        raise ValueError(
            "start_month no puede ser posterior a end_month"
        )

    warmup_month = current_month - relativedelta(months=1)

    logger.info(
        "Preparing context month: %s",
        warmup_month.isoformat(),
    )

    monthly_taxi_pipeline(
        batch_month=warmup_month.isoformat(),
        retention_mode="backfill",
        perform_cleanup=False,
        warmup=True,
    )

    while current_month <= final_month:
        batch_month = current_month.isoformat()

        logger.info(
            "Processing backfill batch: %s",
            batch_month,
        )

        monthly_taxi_pipeline(
            batch_month=batch_month,
            retention_mode="backfill",
            perform_cleanup=False,
        )

        current_month += relativedelta(months=1)

    logger.info("Applying final retention cleanup.")

    cleanup_old_data(
        batch_month=final_month.isoformat(),
        retention_mode="backfill",
    )

    logger.info("Backfill completed successfully.")
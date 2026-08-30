import datetime
import io
import dlt
from dlt.sources.helpers import requests
import pandas as pd
from typing import Generator
from nyc_yellow_taxi.config import settings


def normalize_trip_timestamps(df: pd.DataFrame, source_timezone: str = "America/New_York") -> pd.DataFrame:
    for column in ("tpep_pickup_datetime","tpep_dropoff_datetime"):
        df[column] = (df[column].dt.tz_localize(source_timezone, nonexistent="NaT", ambiguous="NaT").dt.tz_convert("UTC"))
    return df

@dlt.resource(
        name="yellow_trips", 
        merge_key="source_file_month",
        write_disposition={
            "disposition": "merge",
            "strategy":"delete-insert"
            }
        )
def load_parquet_date_range(start_date: datetime.datetime, end_date: datetime.datetime) -> Generator[pd.DataFrame, None, None]:
    for date in pd.date_range(start=start_date, end=end_date, freq='MS'):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date.strftime('%Y-%m')}.parquet"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        df = pd.read_parquet(io.BytesIO(response.content))
        ingested_at = pd.Timestamp.now(tz="UTC")
        df["source_file_month"] = date.date()
        df["ingested_at"] = ingested_at
        df = normalize_trip_timestamps(df, source_timezone="America/New_York")
        yield df

def create_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="nyc_taxi",
        destination="bigquery",
        dataset_name=settings.bronze_dataset,
    )

def ingest_yellow_trips(start_date: datetime.datetime, end_date: datetime.datetime) -> None:
    pipeline = create_pipeline()
    load_info = pipeline.run(load_parquet_date_range(start_date=start_date, end_date=end_date))
    print(load_info)

def main() -> None:
    start_date = datetime.datetime.strptime(
        settings.pipeline_start_date,
        "%Y-%m",
    )
    end_date = datetime.datetime.strptime(
        settings.pipeline_end_date,
        "%Y-%m",
    )

    ingest_yellow_trips(
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    main()

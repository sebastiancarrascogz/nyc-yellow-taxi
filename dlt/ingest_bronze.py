import datetime
import io
import dlt
from dlt.sources.helpers import requests
import pandas as pd
from typing import Generator

@dlt.resource
def load_parquet_date_range(start_date: datetime.datetime, end_date: datetime.datetime) -> Generator[pd.DataFrame, None, None]:
    for date in pd.date_range(start=start_date, end=end_date, freq='MS'):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date.strftime('%Y-%m')}.parquet"
        response = requests.get(url)
        df = pd.read_parquet(io.BytesIO(response.content))
        yield df

pipeline = dlt.pipeline(
    pipeline_name="nyc_taxi",
    destination="bigquery",
    dataset_name="bronze"
)

if __name__ == "__main__":
    load_info = pipeline.run(
        load_parquet_date_range(
            start_date=datetime.datetime(2024, 1, 1),
            end_date=datetime.datetime(2024, 2, 1)
        )
    )
    print(load_info)
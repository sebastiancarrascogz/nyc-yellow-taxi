from dataclasses import dataclass
from pathlib import Path
import os
import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    bigquery_project_id: str
    pipeline_start_date: str
    pipeline_end_date: str
    bronze_dataset: str
    taxi_zones_shape_path: str
    taxi_zones_target_crs: str
    taxi_zones_table_name: str


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    load_dotenv(PROJECT_ROOT / ".env")

    with (PROJECT_ROOT / "config.yaml").open("r") as file:
        config = yaml.safe_load(file) or {}

    return Settings(
        bigquery_project_id=require_env("BIGQUERY_PROJECT_ID"),
        pipeline_start_date=config["pipeline"]["start_date"],
        pipeline_end_date=config["pipeline"]["end_date"],
        bronze_dataset=config["bigquery"]["bronze_dataset"],
        taxi_zones_shape_path=config["taxi_zones"]["source_path"],
        taxi_zones_target_crs=config["taxi_zones"]["target_crs"],
        taxi_zones_table_name=config["taxi_zones"]["table_name"],
    )


settings = load_settings()
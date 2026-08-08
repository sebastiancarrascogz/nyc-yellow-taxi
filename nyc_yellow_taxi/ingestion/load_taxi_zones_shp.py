from pathlib import Path
from tempfile import TemporaryDirectory
import geopandas as gpd
from google.cloud import bigquery
from nyc_yellow_taxi.config import settings

def validate_geodataframe(gdf: gpd.GeoDataFrame) -> None:
    """Validate the basic integrity of a GeoDataFrame."""

    if gdf.empty:
        raise ValueError("GeoDataFrame is empty.")

    if gdf.crs is None:
        raise ValueError("GeoDataFrame does not have a CRS defined.")

    null_geometries = gdf.geometry.isna().sum()

    if null_geometries > 0:
        raise ValueError(
            f"Found {null_geometries} null geometries."
        )

    empty_geometries = gdf.geometry.is_empty.sum()

    if empty_geometries > 0:
        raise ValueError(
            f"Found {empty_geometries} empty geometries."
        )


def repair_geometries(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Repair invalid geometries if any exist."""

    invalid_geometries = (~gdf.geometry.is_valid).sum()

    if invalid_geometries == 0:
        print("All geometries are valid.")
        return gdf

    print(
        f"Found {invalid_geometries} invalid geometries. "
        "Attempting repair..."
    )

    gdf = gdf.copy()
    gdf[gdf.geometry.name] = gdf.geometry.make_valid()

    remaining_invalid = (~gdf.geometry.is_valid).sum()

    if remaining_invalid > 0:
        raise ValueError(
            f"{remaining_invalid} geometries remain invalid "
            "after repair."
        )

    print("Invalid geometries repaired successfully.")

    return gdf


def prepare_taxi_zones(
    shapefile_path: str,
    target_crs: str,
) -> gpd.GeoDataFrame:
    """Read, validate, repair and reproject the taxi zones."""

    shape_path = Path(shapefile_path)

    if not shape_path.exists():
        raise FileNotFoundError(
            f"Shapefile not found: {shape_path}"
        )

    print(f"Reading shapefile: {shape_path}")

    gdf = gpd.read_file(shape_path)

    print(f"Rows loaded: {len(gdf)}")
    print(f"Source CRS: {gdf.crs}")
    print(
        "Geometry types: "
        f"{gdf.geom_type.value_counts().to_dict()}"
    )

    validate_geodataframe(gdf)

    gdf = repair_geometries(gdf)

    print(
        f"Reprojecting from {gdf.crs} "
        f"to {target_crs}..."
    )

    gdf = gdf.to_crs(target_crs)

    validate_geodataframe(gdf)

    if not gdf.geometry.is_valid.all():
        raise ValueError(
            "Invalid geometries found after reprojection."
        )

    print(f"Target CRS: {gdf.crs}")
    print("Taxi zones prepared successfully.")

    return gdf


def load_to_bigquery(
    gdf: gpd.GeoDataFrame,
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> None:
    """Load the GeoDataFrame into BigQuery through GeoParquet."""

    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    print(f"Loading taxi zones into {table_id}...")

    with TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "taxi_zones.parquet"

        gdf.to_parquet(
            parquet_path,
            index=False,
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with parquet_path.open("rb") as source_file:
            load_job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
            )

        load_job.result()

    table = client.get_table(table_id)

    if table.num_rows != len(gdf):
        raise RuntimeError(
            "Row count mismatch after BigQuery load: "
            f"source={len(gdf)}, "
            f"destination={table.num_rows}"
        )

    geometry_name = gdf.geometry.name

    geometry_field = next(
        (
            field
            for field in table.schema
            if field.name == geometry_name
        ),
        None,
    )

    if geometry_field is None:
        raise RuntimeError(
            f"Geometry column '{geometry_name}' "
            "was not found in BigQuery."
        )

    if geometry_field.field_type != "GEOGRAPHY":
        raise RuntimeError(
            f"Geometry column was loaded as "
            f"{geometry_field.field_type}, "
            "expected GEOGRAPHY."
        )

    print(
        f"Successfully loaded {table.num_rows} rows "
        f"into {table_id}."
    )

    print(
        f"Geometry column '{geometry_name}' "
        "loaded as GEOGRAPHY."
    )


def ingest_taxi_zones() -> None:
    """Run the full Bronze ingestion for NYC taxi zones."""

    gdf = prepare_taxi_zones(
        shapefile_path=settings.taxi_zones_shape_path,
        target_crs=settings.taxi_zones_target_crs,
    )

    load_to_bigquery(
        gdf=gdf,
        project_id=settings.bigquery_project_id,
        dataset_id=settings.bronze_dataset,
        table_name=settings.taxi_zones_table_name,
    )


if __name__ == "__main__":
    ingest_taxi_zones()
from pathlib import Path
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


# @task()
# def write_local(url) -> None:
    # os.system(f"wget {url}")

@task()
def fetch(path: str) -> None:
    df = pd.read_csv(path)
    return df

@task()
def write_to_parquet(df: pd.DataFrame, dataset_file: str) -> None:
    path = Path(f"data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload csv.gz parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_path = f"FHV_NY_Taxi_data_2019\{path}"
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path)
    return

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz'
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    df = fetch(dataset_url)
    path = write_to_parquet(df, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int], year: int
):
    for month in months:
        etl_web_to_gcs(year, month)


if __name__ == "__main__":
    months = [i for i in range(1,13)]
    year = 2019
    etl_parent_flow(months, year)


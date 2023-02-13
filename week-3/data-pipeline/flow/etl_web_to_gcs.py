from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

PIPELINE_ROOT_DIR = Path(__file__).parent.absolute().parent
DATA_DIR = Path(PIPELINE_ROOT_DIR, "data")

GCS_BUCKET_BLOCK_NAME = "gcs-de-zoomcamp-bucket"
FHV_TAXI_DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02d}.csv.gz"


@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=7))
def download_url_to_pandas(url: str) -> pd.DataFrame:
    """Download dataset from URL to pandas DataFrame directly"""
    df = pd.read_csv(url)
    print(f"Loaded pandas DataFrame from web. Shape: {df.shape}")
    return df


@task(log_prints=True)
def preprocess_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess pandas DataFrame to make sure data format will be the same"""
    columns = [i.lower() for i in df.columns]
    df.columns = columns

    for col in df.columns:
        if col.endswith("datetime"):
            df[col] = pd.to_datetime(df[col])

    return df


@task(log_prints=True)
def save_pandas_to_parquet(df: pd.DataFrame, parquet_filepath: Path):
    """Save pandas DataFrame to parquet file"""
    if not str(parquet_filepath).endswith(".parquet"):
        raise ValueError("Only .parquet files are allowed for saving")

    if not parquet_filepath.parent.exists():
        Path.mkdir(parquet_filepath.parent, parents=True)

    df.to_parquet(parquet_filepath, index=False, compression="gzip")


@task(log_prints=True)
def upload_parquet_to_gcs_bucket(parquet_filepath: Path, gcs_bucket_block_name: str):
    """Upload parquet file to GCS Bucket"""
    gcp_bucket_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_path = gcp_bucket_block.upload_from_path(
        from_path=parquet_filepath,
        to_path=parquet_filepath.name
    )
    print(f"Successfully loaded to GCS Bucket. Path: `{gcs_path}`")
    return gcs_path


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int):
    """ETL pipeline to load data from WEB to GCS"""
    # extract
    url = FHV_TAXI_DATA_URL.format(year=year, month=month)
    df = download_url_to_pandas(url)
    # transform
    df = preprocess_dataset(df)
    # save
    parquet_filename = f"fhv_tripdata_{year}-{month:02d}.parquet"
    parquet_filepath = Path(DATA_DIR, parquet_filename)
    save_pandas_to_parquet(df, parquet_filepath=parquet_filepath)
    # load
    upload_parquet_to_gcs_bucket(parquet_filepath, GCS_BUCKET_BLOCK_NAME)


def main():
    """All data transfer for selected year"""
    year = 2019
    for month in range(1, 13):
        etl_web_to_gcs(year, month)


if __name__ == "__main__":
    main()

from datetime import timedelta
from pathlib import Path
from typing import Union

import click
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

DATA_DIR = Path(
    Path(__file__).parent.absolute().parent,
    "data"
)
GCS_BUCKET_BLOCK_NAME = "gcs-de-zoomcamp-bucket"


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def web_to_pandas(url: str) -> pd.DataFrame:
    """Load dataset from web to pandas DataFrame"""
    df = pd.read_csv(url)
    print(f"Loaded {df.shape[0]:,d} rows to pandas DataFrame.")
    return df


@task
def process_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess dataset inplace"""

    # modify boolean flag:
    if "store_and_fwd_flag" in df.columns:
        df.store_and_fwd_flag = df.store_and_fwd_flag.map({"Y": True, "N": False})

    # format dates:
    date_cols = [i for i in df.columns if i.endswith("_datetime")]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    # column names to lowercase:
    df.columns = [i.lower() for i in df.columns]
    return df


@task(log_prints=True)
def pandas_to_parquet(df: pd.DataFrame, parquet_filepath: Union[Path, str]):
    """Save pandas DataFrame locally to .parquet file"""
    if not str(parquet_filepath).endswith(".parquet"):
        raise ValueError(f"Only .parquet file type is allowed")

    if not parquet_filepath.parent.exists():
        parquet_filepath.parent.mkdir(parents=True)

    df.to_parquet(parquet_filepath, compression="gzip")
    print(f"There are {df.shape[0]:,d} rows saved to {parquet_filepath.name}")


@task(log_prints=True)
def parquet_to_gcs_bucket(
        parquet_filepath: Union[Path, str],
        gcs_bucket_block_name: str
) -> Union[Path, str]:
    """Download local parquet file to Google Cloud Storage bucket"""
    gcp_bucket_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_path = gcp_bucket_block.upload_from_path(
        from_path=parquet_filepath,
        to_path=parquet_filepath.name
    )
    print(f"Successfully loaded to GCS Bucket. Path: `{gcs_path}`")
    return gcs_path


@flow
def etl_web_to_gcs(taxi_color: str, year: int, month: int):
    """ETL flow to load data from web URL to Google Cloud Storage Bucket"""
    # web -> DataFrame
    taxi_data_url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
        f"{taxi_color}/{taxi_color}_tripdata_{year}-{month:02d}.csv.gz"
    )
    df = web_to_pandas(url=taxi_data_url)

    # preprocessing
    df = process_dataset(df)

    # DataFrame -> .parquet
    parquet_filepath = Path(DATA_DIR, f"{taxi_color}_taxi_trips_{year}_{month:02d}.parquet")
    pandas_to_parquet(df, parquet_filepath=parquet_filepath)

    # local .parquet -> Google Cloud Storage Bucket
    parquet_to_gcs_bucket(parquet_filepath, GCS_BUCKET_BLOCK_NAME)



@click.command
@click.option("--taxi_color",
              type=str,
              help="Color of the taxi cabs")
@click.option("--year",
              type=int,
              help="Year of a taxi trip")
@click.option("--month",
              type=int,
              help="Month of a taxi trip")
def etl_web_to_gcs_command(taxi_color: str, year: int, month: int):
    """CLI command to get load data from web url to Google Cloud Storage bucket"""
    etl_web_to_gcs(taxi_color, year, month)


if __name__ == "__main__":
    etl_web_to_gcs_command()

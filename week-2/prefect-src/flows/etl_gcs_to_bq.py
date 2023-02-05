from pathlib import Path
from typing import Union, List

import click
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

DATA_DIR = Path(
    Path(__file__).parent.absolute().parent,
    "data"
)
GCS_BUCKET_BLOCK_NAME = "gcs-de-zoomcamp-bucket"
GCP_CREDENTIALS_BLOCK_NAME = "gcp-de-zoomcamp"
BQ_DATASET_NAME = "taxi_trips"


@task(retries=3, log_prints=True)
def gcs_to_local(gcs_bucket_block_name: str, filename: str, local_data_dir: Union[Path, str]) -> Path:
    """Download file from Google Cloud Storage and save locally"""
    gcp_bucket_block = GcsBucket.load(gcs_bucket_block_name)
    local_path = gcp_bucket_block.download_object_to_path(
        from_path=filename,
        to_path=Path(local_data_dir, filename)
    )
    print(f"Successfully loaded from GCS to `{local_path}`")
    return local_path


@task(log_prints=True)
def parquet_to_pandas(parquet_filepath: Union[Path, str]) -> pd.DataFrame:
    """Read parquet file to pandas DataFrame"""
    df = pd.read_parquet(parquet_filepath)
    print(f"Loaded {df.shape[0]:,d} rows to pandas DataFrame")
    return df


@task(log_prints=True)
def pandas_to_bigquery(df: pd.DataFrame,
                       gcp_credentials_block_name: str,
                       bq_dataset_name: str,
                       bq_table_name: str,
                       if_exists: str = "append"):
    """Load pandas DataFrame to BigQuery table"""
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    credentials = gcp_credentials_block.get_credentials_from_service_account()
    df.to_gbq(
        destination_table=f"{bq_dataset_name}.{bq_table_name}",
        project_id=gcp_credentials_block.project,
        credentials=credentials,
        chunksize=500_000,
        if_exists=if_exists
    )
    print(f"Saved {df.shape[0]:,d} rows to BigQuery table `{bq_dataset_name}.{bq_table_name}`")


@flow
def etl_gcs_to_bq(taxi_color: str, years: List[int], months: List[int]):
    """ETL flow to load data from Google Cloud Storage Bucket to BigQuery"""
    for year in years:

        for month in months:

            # GCS -> local file.parquet
            local_parquet_filepath = gcs_to_local(
                gcs_bucket_block_name=GCS_BUCKET_BLOCK_NAME,
                filename=f"{taxi_color}_taxi_trips_{year}_{month:02d}.parquet",
                local_data_dir=DATA_DIR
            )

            # .parquet -> pandas DataFrame
            df = parquet_to_pandas(local_parquet_filepath)

            # pandas DataFrame -> BigQuery
            pandas_to_bigquery(
                df=df,
                gcp_credentials_block_name=GCP_CREDENTIALS_BLOCK_NAME,
                bq_dataset_name=BQ_DATASET_NAME,
                bq_table_name=f"{taxi_color}_taxi_trips",
                if_exists="append"
            )


@click.command
@click.option("--taxi_color",
              type=str,
              help="Color of the taxi cabs")
@click.option("--years",
              multiple=True,
              help="List if Years of taxi trips")
@click.option("--months",
              multiple=True,
              help="Month of a taxi trip")
def etl_gcs_to_bq_command(taxi_color: str, years: int, months: int):
    """CLI command to load data from Google Cloud Storage to BigQuery"""
    etl_gcs_to_bq(taxi_color, years, months)


if __name__ == "__main__":
    etl_gcs_to_bq_command()

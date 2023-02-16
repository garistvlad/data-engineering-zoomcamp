from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

PIPELINE_ROOT_DIR = Path(__file__).parent.absolute().parent
DATA_DIR = Path(PIPELINE_ROOT_DIR, "data")

GCS_BUCKET_BLOCK_NAME = "gcs-de-zoomcamp-bucket"
TAXI_DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"


@task(log_prints=True)
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

        if "count" in col:
            df[col] = df[col].fillna(0).astype(int)

    return df


@task(log_prints=True)
def save_pandas_to_csv(df: pd.DataFrame, csv_filepath: Path):
    """Save pandas DataFrame to CSV file"""
    if not csv_filepath.parent.exists():
        Path.mkdir(csv_filepath.parent, parents=True)

    df.to_csv(csv_filepath, index=False, compression="gzip")


@task(log_prints=True)
def upload_parquet_to_gcs_bucket(csv_filepath: Path, gcs_bucket_block_name: str):
    """Upload CSV file to GCS Bucket"""
    gcp_bucket_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_path = gcp_bucket_block.upload_from_path(
        from_path=csv_filepath,
        to_path=csv_filepath.name
    )
    print(f"Successfully loaded to GCS Bucket. Path: `{gcs_path}`")
    return gcs_path


@flow(log_prints=True)
def etl_web_to_gcs(taxi_type: str, year: int, month: int):
    """ETL pipeline to load data from WEB to GCS"""
    # extract
    url = TAXI_DATA_URL.format(taxi_type=taxi_type, year=year, month=month)
    df = download_url_to_pandas(url)
    # transform
    df = preprocess_dataset(df)
    # save
    csv_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    csv_filepath = Path(DATA_DIR, taxi_type, csv_filename)
    save_pandas_to_csv(df, csv_filepath=csv_filepath)
    # load
    upload_parquet_to_gcs_bucket(csv_filepath, GCS_BUCKET_BLOCK_NAME)


def main():
    """All data transfer for selected year"""
    years = [2019, 2020]
    taxi_types = ["fhv", "green", "yellow"]
    months = list(range(1, 13))

    for year in years:
        for taxi_type in taxi_types:
            if year == 2020 and taxi_type == "fhv":
                continue

            for month in months:
                etl_web_to_gcs(taxi_type, year, month)


if __name__ == "__main__":
    main()

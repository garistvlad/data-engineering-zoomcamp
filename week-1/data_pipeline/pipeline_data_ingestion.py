import logging
import os
from pathlib import Path
import time

import click
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

CURRENT_DIR = Path(__file__).parent.absolute()


def setup_logger(name: str) -> logging.Logger:
    """TIME - NAME - LEVEL - MESSAGE"""
    # logger
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.INFO)
    # handler
    console_handler = logging.StreamHandler()
    # formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def get_postgres_url_from_env(env_filepath: Path) -> str:
    """Create postgres connection URL from env filepath"""
    load_dotenv(env_filepath)
    pg_user = os.environ.get("POSTGRES_USER")
    pg_pass = os.environ.get("POSTGRES_PASSWORD")
    pg_host = os.environ.get("POSTGRES_HOST")
    pg_port = os.environ.get("POSTGRES_PORT")
    pg_db = os.environ.get("POSTGRES_DB")
    url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    return url


def process_dataset(df: pd.DataFrame):
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


def run_pipeline(env_filepath: Path,
             dataset_filepath: Path,
             tablename: str):
    """Pipeline to ingest data into postgres"""
    start = time.perf_counter()
    logger = setup_logger("DATA-TO-POSTGRES-PIPELINE")
    logger.info("Start data loading to postgres...")

    # Read
    logger.info("Start reading dataset to pandas...")
    df = pd.read_csv(dataset_filepath)
    logger.info(f"Done. Shape: {df.shape}")

    # Preprocess
    logger.info("Start preprocessing DataFrame...")
    process_dataset(df)
    logger.info("Done.")

    # Load to Postgres
    logger.info("Start inserting to Postgres")
    pg_engine = create_engine(url=get_postgres_url_from_env(env_filepath))
    df.to_sql(
        name=tablename,
        con=pg_engine,
        index=False,
        if_exists="replace",
        chunksize=100_000
    )
    logger.info("Done")

    end = time.perf_counter()
    logger.info(f"Finished loading data to postgres. Time: {(end - start):.2f}s")


@click.command()
@click.option("--env",
              type=str,
              help="Filepath with .env file, containing PostgreSQL connection config")
@click.option("--dataset",
              type=str,
              help="Filepath with dataset.csv being read for further ingestion")
@click.option("--tablename",
              type=str,
              help="Table name inside postgres to load dataset into")
def run_pipeline_command(env: str, dataset: str, tablename: str):
    """CLI command to run pipeline with arguments"""
    run_pipeline(
        env_filepath=Path(CURRENT_DIR, env),
        dataset_filepath=Path(CURRENT_DIR, dataset),
        tablename=tablename
    )


if __name__ == "__main__":
    run_pipeline_command()

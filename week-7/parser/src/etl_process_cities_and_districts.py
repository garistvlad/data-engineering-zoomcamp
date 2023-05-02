"""
HOWTO Launch:
python src/etl_process_cities_and_districts.py \
    --pipeline_params_filepath="configs/pipeline_params.yaml"

Artefacts:
    - cities.parquet
    - districts.parquet
"""

from datetime import datetime
from pathlib import Path
import sys

import click
import pandas as pd

ROOT_DIR = Path(__file__).parent.absolute().parent
sys.path.append(str(ROOT_DIR))

from params.pipeline_params import PipelineParams
from utils.logger import setup_logger


def process_cities(df_cities: pd.DataFrame) -> pd.DataFrame:
    """Process cities parsed"""
    df_cities.rename({"id": "city_id"}, axis=1, inplace=True)
    return df_cities


def process_districts(df_districts: pd.DataFrame) -> pd.DataFrame:
    """Process districts parsed"""
    df_districts["city_id"] = df_districts.city.apply(lambda x: x.get("id")).astype(int)
    df_districts.rename({"id": "district_id"}, axis=1, inplace=True)
    df_districts.drop("city", axis=1, inplace=True)
    return df_districts


@click.command
@click.option("--pipeline_params_filepath",
              required=True,
              type=str,
              help="Filepath with YAML overall pipeline params")
def main(pipeline_params_filepath: str):
    logger = setup_logger("PROCESS-CITIES-AND-DISTRICTS")

    logger.info("Start processing cities and districts")
    start = datetime.now()

    logger.info("Start loading YAML params")
    params = PipelineParams.from_yaml(pipeline_params_filepath)
    logger.info("Done")

    if not Path(params.processed_data_dir).exists():
        Path(params.processed_data_dir).mkdir(parents=True, exist_ok=True)

    # city
    logger.info("Start processing cities")
    df_cities = pd.read_parquet(Path(params.raw_data_dir, params.cities_filename), engine="pyarrow")
    df_cities = process_cities(df_cities)
    cities_processed_filepath = Path(params.processed_data_dir, params.cities_filename)
    df_cities.to_parquet(cities_processed_filepath, engine="pyarrow")
    logger.info(f"Done. Saved {df_cities.shape[0]} cities to {cities_processed_filepath}")

    # district
    logger.info("Start processing districts")
    df_districts = pd.read_parquet(Path(params.raw_data_dir, params.districts_filename), engine="pyarrow")
    df_districts = process_districts(df_districts)
    districts_processed_filepath = Path(params.processed_data_dir, params.districts_filename)
    df_districts.to_parquet(districts_processed_filepath, engine="pyarrow")
    logger.info(f"Done. Saved {df_districts.shape[0]} districts to {districts_processed_filepath}")

    end = datetime.now()
    logger.info(f"Done. Time: {end - start}")


if __name__ == "__main__":
    main()

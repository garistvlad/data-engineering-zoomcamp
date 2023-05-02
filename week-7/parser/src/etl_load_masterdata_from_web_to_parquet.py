"""Load masterdata from WEB to parquet
HOWTO Launch:
python src/etl_load_masterdata_from_web_to_parquet.py \
    --pipeline_params_filepath="configs/pipeline_params.yaml"

Artefacts:
    - rubrics.parquet
    - cities.parquet
    - districts.parquet
"""

from datetime import datetime
from pathlib import Path
import sys
from typing import Union, Dict, List

import click
import httpx
from marshmallow import Schema
import pandas as pd

ROOT_DIR = Path(__file__).parent.absolute().parent
sys.path.append(str(ROOT_DIR))

from params.pipeline_params import PipelineParams
from utils.logger import setup_logger
from schemas.rubric import RubricSchema
from schemas.city import CitySchema
from schemas.district import DistrictSchema

RUBRICS_URLS = [
    "https://www.bazaraki.com/api/items/rubrics/",
    "https://www.bazaraki.com/api/items/rubrics/19/",
    "https://www.bazaraki.com/api/items/rubrics/3527/",
]
CITIES_URL = "https://www.bazaraki.com/api/items/cities/"
DISTRICTS_URL = "https://www.bazaraki.com/api/items/all_cities_districts/"


def load_json_from_web(url: str) -> Union[Dict, List]:
    """Load JSON from URL"""
    resp = httpx.get(url)
    resp.raise_for_status()
    data = resp.json()
    if type(data) == dict and "results" in data:
        data = data["results"]

    if type(data) == list and len(data) > 1 and "city_districts" in data[0]:
        result = []
        for i in data:
            result += i["city_districts"]
        return result

    return data


def deserialize_json(data: Union[Dict, List], target_schema: Schema) -> List[Schema]:
    """Deserialize JSON based on schema defined"""
    if type(data) == list:
        return [target_schema.load(i) for i in data]
    return [target_schema.load(data)]


def to_pandas(items: List[Schema]) -> pd.DataFrame:
    """Schema ovjects to pandas DataFrame"""
    df = pd.DataFrame(items)
    return df


def task_url_to_pandas(url: str, target_schema: Schema) -> pd.DataFrame:
    """Full flow: from URL to pandas DataFrame"""
    data = load_json_from_web(url=url)
    items = deserialize_json(data, target_schema)
    df = to_pandas(items)
    return df


def load_rubrics_from_web_to_pandas() -> pd.DataFrame:
    """Load rubrics from bazaraki, combine to one DataFrame"""
    df_rubrics = pd.DataFrame()
    for url in RUBRICS_URLS:
        # load single rubric to pandas
        df = task_url_to_pandas(url=url, target_schema=RubricSchema)
        # combine rubrics
        df_rubrics = pd.concat((df_rubrics, df), axis=0, ignore_index=True)
    return df_rubrics


def pandas_to_parquet(df: pd.DataFrame, filepath: Path):
    """Save pandas DataFrame to .parquet"""
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath, engine="pyarrow")


@click.command
@click.option("--pipeline_params_filepath",
              required=True,
              type=str,
              help="Filepath with YAML overall pipeline params")
def main(pipeline_params_filepath: str):
    logger = setup_logger("LOAD-MASTERDATA")

    logger.info("Start loading MASTERDATA from web.")
    start = datetime.now()

    logger.info("Start loading YAML params")
    params = PipelineParams.from_yaml(pipeline_params_filepath)
    logger.info("Done")

    # rubrics
    df = load_rubrics_from_web_to_pandas()
    rubrics_filepath = Path(params.raw_data_dir, params.rubrics_filename)
    pandas_to_parquet(df, rubrics_filepath)
    logger.info(f"`{rubrics_filepath.name}` saved to {rubrics_filepath}")

    # others
    for url, item_filename, schema in zip(
            [CITIES_URL, DISTRICTS_URL],
            [params.cities_filename, params.districts_filename],
            [CitySchema, DistrictSchema]
    ):
        # load to pandas
        df = task_url_to_pandas(url=url, target_schema=schema)
        # save to .parquet
        target_filepath = Path(params.raw_data_dir, item_filename)
        pandas_to_parquet(df, target_filepath)
        logger.info(f"`{item_filename}` saved to {target_filepath}")

    end = datetime.now()
    logger.info(f"Done. Time: {end - start}")


if __name__ == "__main__":
    main()

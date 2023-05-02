"""
Load & Save advertisements from WEB to parquet files.
HOWTO launch:
python src/etl_load_ads_from_web_to_parquet.py \
    --pipeline_params_filepath="configs/pipeline_params.yaml" \
    --full
"""

from datetime import datetime
from pathlib import Path
import shutil
import sys
from typing import Dict, List
import time

import click
import httpx
from marshmallow import Schema
import pandas as pd

PARSER_DIR = Path(__file__).parent.absolute().parent
sys.path.append(str(PARSER_DIR))

ADS_BASE_URL = "https://www.bazaraki.com/api/items/"

from params.pipeline_params import PipelineParams
from utils.json_helper import save_json, load_json
from utils.logger import setup_logger
from schemas.advertising import AdvertisingSchema


def load_json_with_ads_from_web(base_url: str, rubric_id: int, page_size: int, page_num: int) -> Dict:
    """Load JSON from URL"""
    query_params = {
        "ordering": "oldest",
        "rubric": rubric_id,
        "page": page_num,
        "page_size": page_size,
    }
    resp = httpx.get(base_url, params=query_params)
    resp.raise_for_status()
    data = resp.json()
    return data


def get_realty_rubric_ids(rubrics_processed_filepath: Path) -> List[int]:
    """Get realty rubric IDs previously parsed from WEB"""
    df_rubrics = pd.read_parquet(
        rubrics_processed_filepath,
        engine="pyarrow"
    )
    if "id" not in df_rubrics:
        raise KeyError("There is no `id` field inside rubrics.parquet.")
    return df_rubrics["id"].to_list()


def load_rubric_ads_from_web_to_json(
        base_url: str,
        rubric_id: int,
        page_size: int,
        data_path: Path,
        num_retries: int,
        is_full: bool = False
) -> Path:
    """Load data from web for rubric iteratively page by page"""

    data_dir = Path(data_path, f"{rubric_id}")

    if data_dir.exists():
        shutil.rmtree(data_dir)

    page_num = 1
    retries = 1
    while True:
        try:
            data = load_json_with_ads_from_web(
                base_url=base_url,
                rubric_id=rubric_id,
                page_size=page_size,
                page_num=page_num
            )

        except httpx.HTTPError:
            print(f"ERROR with loading page #{page_num} for rubric #{rubric_id}. Attempt #{retries}/{num_retries}")
            retries += 1
            if retries > num_retries:
                break
            continue

        if "detail" in data and data.get("detail") == "Invalid page.":
            break

        save_json(
            data,
            Path(data_dir, f"page_{page_num}.json")
        )

        page_num += 1
        retries = 1

        # TODO: load data with async from web
        time.sleep(0.5)

        if not is_full:
            break

    return data_dir


def parse_adv_json_to_schema(data: Dict, schema: Schema) -> List[Schema]:
    """Load data from JSON to predefined Schema"""

    if "results" not in data:
        raise KeyError("'results' not found in data")

    result = [schema.load(i) for i in data["results"]]
    return result


def parse_adv_schema_to_pandas(data: List[Schema]) -> pd.DataFrame:
    """Parse cities to DataFrame. No specific processing"""
    df = pd.DataFrame(data)
    if "attrs" in df.columns:
        df["attrs"] = df["attrs"].to_string()
    return df


def rubric_jsons_to_dataframe(rubric_data_dir: Path) -> pd.DataFrame:
    """Grab all JSONs from folder to single DataFrame"""
    df = pd.DataFrame()
    for raw_filepath in rubric_data_dir.glob(pattern=r"*.json"):
        # load
        raw_data = load_json(raw_filepath)

        # process:
        processed_data = parse_adv_json_to_schema(
            data=raw_data,
            schema=AdvertisingSchema
        )

        # to pandas:
        df = pd.concat((
            df,
            parse_adv_schema_to_pandas(processed_data),
        ), ignore_index=True, axis=0)

    return df


@click.command()
@click.option("--pipeline_params_filepath",
              required=True,
              type=str,
              help="Filepath with YAML overall pipeline params")
@click.option("--full",
              is_flag=True,
              show_default=True,
              default=False,
              help="Flag: full data extract (--full) or incremental (default)")
def main(pipeline_params_filepath: str, full: bool):

    logger = setup_logger("LOAD-ADS")

    logger.info("Start loading & parsing ADS.")
    start = datetime.now()

    logger.info("Start loading YAML params")
    params = PipelineParams.from_yaml(pipeline_params_filepath)
    logger.info("Done")

    rubrics_processed_filepath = Path(params.processed_data_dir, params.rubrics_filename)
    realty_rubrics = get_realty_rubric_ids(rubrics_processed_filepath)
    if full:
        rubrics_base_dir = Path(params.raw_data_dir, params.advertisements_full_data_dir_name)
    else:
        rubrics_base_dir = Path(params.raw_data_dir, params.advertisements_incremental_data_dir_name)
    for rubric_id in realty_rubrics:

        # extract from web
        logger.info(f"Start loading rubric #`{rubric_id}` from web")
        rubric_data_dir = load_rubric_ads_from_web_to_json(
            base_url=ADS_BASE_URL,
            rubric_id=rubric_id,
            page_size=params.web_page_size,
            data_path=rubrics_base_dir,
            num_retries=params.num_retries,
            is_full=full
        )
        logger.info(f"Done. Rubric #`{rubric_id}` JSON files saved to `{rubric_data_dir}`.")

        # load to pandas DataFrame
        logger.info(f"Start grabbing rubric #`{rubric_id}` JSON files")
        df_ads = rubric_jsons_to_dataframe(rubric_data_dir=rubric_data_dir)
        logger.info(f"Loaded Ads for rubric #{rubric_id} to DataFrame. Shape: {df_ads.shape}")

        # save to parquet
        if df_ads.shape[0] > 0:
            parquet_filepath = Path(rubric_data_dir.parent, f"rubric_{rubric_id}.parquet")
            df_ads.to_parquet(
                parquet_filepath,
                engine="pyarrow"
            )
            logger.info(f"Ads for rubric #{rubric_id} saved to {parquet_filepath}.")
        else:
            logger.info(f"There are no Ads for rubric #{rubric_id}. Skipped...")

    end = datetime.now()
    logger.info(f"Done. Time: {end - start}")


if __name__ == "__main__":
    main()

"""
HOWTO Launch:
python3 src/etl_process_advertisement_attributes.py \
    --pipeline_params_filepath="configs/pipeline_local_params.yaml" \
    --full

Artefacts:
    - rubric_{rubric_id}.parquet
"""
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict

import click
import pandas as pd

ROOT_DIR = Path(__file__).parent.absolute().parent
sys.path.append(str(ROOT_DIR))

from params.pipeline_params import PipelineParams
from utils.logger import setup_logger
from utils.parquet_helper import read_parquet, save_parquet


def load_all_attributes_from_json_folder(data_dir: Path) -> Dict:
    """Combine .json files from folder to a single Dict"""
    attr_dict = dict()
    for path_i in sorted(data_dir.glob(pattern="*.json")):
        with open(path_i, "r") as input_stream:
            ads = json.load(input_stream)
        # load attributes:
        attributes = {i["id"]: i["attrs"] for i in ads["results"]}
        # update existing:
        attr_dict.update(attributes)

    return attr_dict


def process_attribute_column(
        series: pd.Series,
        col_name: str,
        rubric_features_dict: Dict,
) -> pd.Series:
    """Process features based on bazaraki predefined Enums"""
    if col_name not in rubric_features_dict.index:
        return series

    if rubric_features_dict.loc[col_name].feature_type in ("Integer", "String"):
        return series

    if rubric_features_dict.loc[col_name].feature_type in ("Integer choices", "String choices"):
        return series.apply(lambda x: rubric_features_dict.loc[col_name].feature_choices.get(str(x)))

    else:
        # TODO Implementation...
        return series


def preprocessing_attributes_dict_to_dataframe(
        attributes: Dict,
        rubric_id: int,
        df_rubric_features: pd.DataFrame,
) -> pd.DataFrame:
    """Process attribute Dict to pandas DataFrame with cleaned features"""

    rubric_features_dict = df_rubric_features[
        df_rubric_features.rubric_id == rubric_id
    ].set_index("feature_name")

    df_attributes = pd.DataFrame(attributes).T

    for col_name in df_attributes.columns:
        df_attributes[col_name] = process_attribute_column(
            series=df_attributes[col_name],
            col_name=col_name,
            rubric_features_dict=rubric_features_dict,
        )

        df_attributes.rename(
            {col_name: col_name.replace("attrs__", "").replace("-", "_")},
            axis=1,
            inplace=True
        )

    # final processing:
    df_attributes.reset_index(names=["advertisement_id"], inplace=True)
    df_attributes["rubric_id"] = rubric_id

    return df_attributes



@click.command
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

    logger = setup_logger("PROCESS-AD-FEATURES")

    logger.info("Start processign Ad Features")
    start = datetime.now()

    logger.info("Start loading YAML params")
    params = PipelineParams.from_yaml(pipeline_params_filepath)
    logger.info("Done")

    logger.info("Start loading ads from JSON folder")
    if full:
        realty_ads_raw_data_dir = Path(params.raw_data_dir, params.advertisements_full_data_dir_name)
        realty_ads_processed_data_dir = Path(params.processed_data_dir, params.advertisements_full_data_dir_name)
        features_processed_data_dir = Path(params.processed_data_dir, params.ad_features_full_data_dir_name)
    else:
        realty_ads_raw_data_dir = Path(params.raw_data_dir, params.advertisements_incremental_data_dir_name)
        realty_ads_processed_data_dir = Path(params.processed_data_dir, params.advertisements_incremental_data_dir_name)
        features_processed_data_dir = Path(params.processed_data_dir, params.ad_features_incremental_data_dir_name)

    if not realty_ads_processed_data_dir.exists():
        realty_ads_processed_data_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Load rubrics masterdata first...")
    df_rubrics = read_parquet(Path(params.processed_data_dir, params.rubrics_filename))

    logger.info("Load rubric features masterdata first...")
    df_rubric_features = read_parquet(Path(params.processed_data_dir, params.rubric_features_filename))

    # load rubric by rubric:
    for rubric_id in df_rubrics["id"]:
        # load JSON data
        json_data_dir = Path(realty_ads_raw_data_dir, f"{rubric_id}")
        attributes = load_all_attributes_from_json_folder(data_dir=json_data_dir)

        # process JSON to DataFrame:
        df_attributes = preprocessing_attributes_dict_to_dataframe(
            attributes=attributes,
            rubric_id=rubric_id,
            df_rubric_features=df_rubric_features
        )
        logger.info(f"Processed rubric #{rubric_id} data to pandas DataFrame. Shape: {df_attributes.shape}")

        # save to .parquet
        features_filepath_i = Path(features_processed_data_dir, f"rubric_{rubric_id}.parquet")
        save_parquet(df_attributes, features_filepath_i)
        logger.info(f"Saved rubric #{rubric_id} data to .parquet")

    end = datetime.now()
    logger.info(f"Done. Time: {end - start}")


if __name__ == "__main__":
    main()

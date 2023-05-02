"""
HOWTO Launch:
python src/etl_process_rubrics_and_features.py \
    --pipeline_params_filepath="configs/pipeline_params.yaml"

Artefacts:
    - realty_rubric_features.parquet
    - realty_rubrics.parquet
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
from schemas.rubric import RubricFeatureSchema


@click.command
@click.option("--pipeline_params_filepath",
              required=True,
              type=str,
              help="Filepath with YAML overall pipeline params")
def main(pipeline_params_filepath: Path):
    logger = setup_logger("PROCESS-RUBRICS")

    logger.info("Start processign Rubrics")
    start = datetime.now()

    logger.info("Start loading YAML params")
    params = PipelineParams.from_yaml(pipeline_params_filepath)
    logger.info("Done")

    # load from parquet:
    df_rubrics = pd.read_parquet(
        Path(params.raw_data_dir, params.rubrics_filename),
        engine="pyarrow"
    )

    # realty rubrics only:
    df_realty_rubrics = df_rubrics[
        df_rubrics.parent_id.isin([19, 3527])
    ][["id", "name", "slug", "path", "rubric_features"]]

    # get rubric features. Key: (rubric_id + feature_name)
    df_realty_rubric_features = pd.DataFrame()
    for i, row in df_realty_rubrics.iterrows():
        df_i = pd.DataFrame([RubricFeatureSchema.load(i) for i in row.rubric_features]).assign(
            rubric_id=row.id
        )
        df_realty_rubric_features = pd.concat(
            (df_realty_rubric_features, df_i),
            axis=0, ignore_index=True
        )
    # process feature choices:
    df_realty_rubric_features.feature_choices = (
        df_realty_rubric_features.feature_choices.apply(
            lambda x: {i["key"]: i["value"] for i in x}
        )
    )
    # save features to parquet:
    if not Path(params.processed_data_dir).exists():
        Path(params.processed_data_dir).mkdir(parents=True, exist_ok=True)

    realty_rubric_features_filepath = Path(params.processed_data_dir, params.rubric_features_filename)
    df_realty_rubric_features.to_parquet(
        realty_rubric_features_filepath,
        engine="pyarrow"
    )
    logger.info(f"Realty rubric features saved to parquet. Path: {realty_rubric_features_filepath}")

    # final format & save rubrics:
    realty_rubrics_filepath = Path(params.processed_data_dir, params.rubrics_filename)
    df_realty_rubrics.drop("rubric_features", axis=1, inplace=True)
    df_realty_rubrics.to_parquet(
        realty_rubrics_filepath,
        engine="pyarrow"
    )
    logger.info(f"Realty rubrics saved to parquet. Path: {realty_rubrics_filepath}")

    end = datetime.now()
    logger.info(f"Done. Time: {end - start}")


if __name__ == "__main__":
    main()

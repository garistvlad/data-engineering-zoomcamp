"""
HOWTO Launch:
python src/etl_process_advertisements.py \
    --pipeline_params_filepath="configs/pipeline_params.yaml" \
    --full

Artefacts:
    - users.parquet
    - images.parquet
    - image_to_advertisement_mapping.parquet
    - advertisements.parquet
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
from schemas.user import UserSchema
from schemas.image import ImageSchema


def load_load_all_parquet_files_from_folder(data_dir: Path) -> pd.DataFrame:
    """Combine .parquet files from folder to single DataFrame"""
    df_ads = pd.DataFrame()
    for path_i in sorted(data_dir.glob(pattern="*.parquet")):
        df_i = pd.read_parquet(path_i)
        df_ads = pd.concat((df_ads, df_i), ignore_index=True, axis=0)
    return df_ads


def extract_users_from_ads(df_ads: pd.DataFrame) -> pd.DataFrame:
    """Users loaded from Ads"""
    user_list = df_ads.user \
        .apply(lambda x: UserSchema.load(x)) \
        .drop_duplicates() \
        .to_list()

    df_users = pd.DataFrame(user_list)
    # process users:
    df_users = df_users.rename({
        "id": "user_id",
        "joined": "registration_date",
    }, axis=1)
    # format date
    df_users["registration_date"] = pd.to_datetime(df_users["registration_date"])
    return df_users


def extract_images_from_ads(df_ads: pd.DataFrame) -> pd.DataFrame:
    """Images loaded from Ads"""
    image_list = df_ads.images \
        .apply(lambda x: [ImageSchema.load(i) for i in x]) \
        .apply(pd.Series) \
        .stack() \
        .to_list()

    df_images = pd.DataFrame(image_list) \
        .drop_duplicates() \
        .reset_index(drop=True)

    df_images = df_images.rename({
        "id": "image_id",
        "url": "compressed_url",
        "orig": "origiral_url",
    }, axis=1)

    return df_images


def extract_image_to_ad_mapping(df_ads: pd.DataFrame) -> pd.DataFrame:
    """Image to Ad mapping"""
    image_to_ad_list = df_ads \
        .apply(
        lambda x: [[i["id"], x["id"]] for i in x.images],
        axis=1
    )\
        .apply(pd.Series) \
        .stack() \
        .to_list()

    df_image_to_ad = pd.DataFrame(
        image_to_ad_list,
        columns=["image_id", "advertisement_id"]
    )
    return df_image_to_ad


def final_ads_cleaning(df_ads: pd.DataFrame) -> pd.DataFrame:
    """Format ads before saving final result"""
    # rename some columns
    df_ads = df_ads.rename({
        "id": "advertisement_id",
        "rubric": "rubric_id",
        "city": "city_id"
    }, axis=1)

    # add user_id link:
    df_ads["user_id"] = df_ads.user.apply(lambda x: x.get("id"))

    # parse district_id
    df_ads["district_id"] = df_ads.city_districts.apply(
        lambda x: x[0] if len(x) > 0 else None
    )

    # drop unused columns:
    df_ads.drop(["city_districts", "user", "images"], axis=1, inplace=True)
    return df_ads


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

    logger = setup_logger("PROCESS-ADS")

    logger.info("Start processign Ads")
    start = datetime.now()

    logger.info("Start loading YAML params")
    params = PipelineParams.from_yaml(pipeline_params_filepath)
    logger.info("Done")

    logger.info("Start loading ads from folder")
    if full:
        realty_ads_raw_data_dir = Path(params.raw_data_dir, params.advertisements_full_data_dir_name)
        realty_ads_processed_data_dir = Path(params.processed_data_dir, params.advertisements_full_data_dir_name)
    else:
        realty_ads_raw_data_dir = Path(params.raw_data_dir, params.advertisements_incremental_data_dir_name)
        realty_ads_processed_data_dir = Path(params.processed_data_dir, params.advertisements_incremental_data_dir_name)

    if not realty_ads_processed_data_dir.exists():
        realty_ads_processed_data_dir.mkdir(parents=True, exist_ok=True)

    df_ads = load_load_all_parquet_files_from_folder(realty_ads_raw_data_dir)
    logger.info(f"Done. Shape: {df_ads.shape}")

    # parse users
    logger.info("Start USERS extract")
    df_users = extract_users_from_ads(df_ads)
    users_parquet_filepath = Path(realty_ads_processed_data_dir, params.users_filename)
    df_users.to_parquet(users_parquet_filepath, engine="pyarrow")
    logger.info(f"Done. Saved {df_users.shape[0]} users to `{users_parquet_filepath}`")

    # parse images
    logger.info("Start IMAGES extract")
    df_images = extract_images_from_ads(df_ads)
    image_parquet_filepath = Path(realty_ads_processed_data_dir, params.images_filename)
    df_images.to_parquet(image_parquet_filepath, engine="pyarrow")
    logger.info(f"Done. Saved {df_images.shape[0]} images to `{image_parquet_filepath}`")

    # parse image-to-advertisement mapping
    logger.info("Start Image-to-Advertisement extract")
    df_image_to_advertisement = extract_image_to_ad_mapping(df_ads)
    image_to_ad_parquet_filepath = Path(realty_ads_processed_data_dir, params.image_to_advertisement_mapping_filename)
    df_image_to_advertisement.to_parquet(image_to_ad_parquet_filepath, engine="pyarrow")
    logger.info(f"Done. Saved {df_image_to_advertisement.shape[0]} image-to-ad to `{image_to_ad_parquet_filepath}`")

    # clean & save ads
    logger.info("Start ads cleaning")
    df_ads = final_ads_cleaning(df_ads)
    processed_ads_filepath = Path(realty_ads_processed_data_dir, params.advertisements_filename)
    df_ads.to_parquet(processed_ads_filepath, engine="pyarrow")
    logger.info(f"Done. Saved {df_ads.shape[0]} advertisements to `{processed_ads_filepath}`")

    end = datetime.now()
    logger.info(f"Done. Time: {end - start}")


if __name__ == "__main__":
    main()

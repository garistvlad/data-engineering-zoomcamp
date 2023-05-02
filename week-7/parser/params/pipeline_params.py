from pathlib import Path
from dataclasses import dataclass

import yaml


@dataclass
class PipelineParams:
    """Config with overall pipeline params"""
    raw_data_dir: str
    processed_data_dir: str
    # names for folders
    advertisements_full_data_dir_name: str
    advertisements_incremental_data_dir_name: str
    # feature folder names:
    ad_features_full_data_dir_name: str
    ad_features_incremental_data_dir_name: str
    # names for files
    cities_filename: str
    districts_filename: str
    rubrics_filename: str
    rubric_features_filename: str
    users_filename: str
    images_filename: str
    advertisements_filename: str
    image_to_advertisement_mapping_filename: str
    # web parser config
    web_page_size: int
    num_retries: int

    @classmethod
    def from_yaml(cls, yaml_filepath: Path) -> "PipelineParams":
        if not Path(yaml_filepath).exists():
            raise FileNotFoundError(f"Could not find file: `{yaml_filepath}`")

        with open(yaml_filepath, "r") as input_stream:
            data = yaml.safe_load(input_stream)

        return cls(**data)

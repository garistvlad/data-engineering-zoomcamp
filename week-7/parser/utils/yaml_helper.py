from pathlib import Path
from typing import Union, Any

import yaml


def read_yaml(filepath: Union[str, Path]) -> Any:
    """Load .yaml file to python object"""
    with open(filepath, "r") as input_stream:
        result = yaml.safe_load(input_stream)

    return result

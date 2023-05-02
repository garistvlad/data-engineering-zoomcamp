from pathlib import Path
import json
from typing import Union


def load_json(filepath: Union[str, Path]) -> object:
    """Load pickled object from file"""
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Could not find file {filepath.name} inside {filepath.parent}")

    if not filepath.name.endswith(".json"):
        raise ValueError("Filename should be ended with .json")

    with open(filepath, "r") as input_filestream:
        result = json.load(input_filestream)

    return result


def save_json(obj: object, filepath: Union[str, Path]):
    """Save object to pickle file"""
    filepath = Path(filepath)
    if not filepath.parent.exists():
        filepath.parent.mkdir(exist_ok=True, parents=True)

    if not filepath.name.endswith(".json"):
        raise ValueError("Only .json files are allowed")

    with open(filepath, "w") as output_filestream:
        json.dump(obj, output_filestream)

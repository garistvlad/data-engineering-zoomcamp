from pathlib import Path
import pickle
from typing import Union


def load_pickle(filepath: Union[str, Path]) -> object:
    """Load pickled object from file"""
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Could not find file {filepath.name} inside {filepath.parent}")

    if not filepath.name.endswith(".pickle"):
        raise ValueError("Filename should be ended with .pickle")

    with open(filepath, "rb") as input_filestream:
        result = pickle.load(input_filestream)

    return result


def save_pickle(obj: object, filepath: Union[str, Path]):
    """Save object to pickle file"""
    filepath = Path(filepath)
    if not filepath.parent.exists():
        filepath.parent.mkdir(exist_ok=True, parents=True)

    if not filepath.name.endswith(".pickle"):
        raise ValueError("Only .pickle files are allowed")

    with open(filepath, "wb") as output_filestream:
        pickle.dump(obj, output_filestream, protocol=pickle.HIGHEST_PROTOCOL)

from pathlib import Path
from typing import Union

import pandas as pd


def read_parquet(filepath: Union[str, Path]) -> pd.DataFrame:
    """Load pandas DataFrame object from parquet file"""
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Could not find file {filepath.name} inside {filepath.parent}")

    if not filepath.name.endswith(".parquet"):
        raise ValueError("Filename should be ended with .pickle")

    df = pd.read_parquet(filepath, engine="pyarrow")
    return df


def save_parquet(df: pd.DataFrame, filepath: Union[str, Path]):
    """Save pandas DataFrame to parquet file"""
    filepath = Path(filepath)
    if not filepath.parent.exists():
        filepath.parent.mkdir(exist_ok=True, parents=True)

    if not filepath.name.endswith(".parquet"):
        raise ValueError("Only .parquet files are allowed")

    df.to_parquet(filepath, engine="pyarrow")

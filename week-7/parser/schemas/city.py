from dataclasses import dataclass
from pathlib import Path
import sys

import marshmallow_dataclass

CURRENT_DIR = Path(__file__).parent.absolute()
sys.path.append(str(CURRENT_DIR))

from coordinates import Coordinates


@dataclass
class City:
    name: str
    id: int
    slug: str
    coordinates: Coordinates


CitySchema = marshmallow_dataclass.class_schema(City)()

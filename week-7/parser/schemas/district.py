from dataclasses import dataclass
from pathlib import Path
import sys
from typing import List

import marshmallow_dataclass

CURRENT_DIR = Path(__file__).parent.absolute()
sys.path.append(str(CURRENT_DIR))

from coordinates import Coordinates
from city import City


@dataclass
class District:
    name: str
    city: City
    id: int
    slug: str
    post_codes: List[int]
    coordinates: Coordinates


DistrictSchema = marshmallow_dataclass.class_schema(District)()

from dataclasses import dataclass
from pathlib import Path
import sys

import marshmallow_dataclass

CURRENT_DIR = Path(__file__).parent.absolute()
sys.path.append(str(CURRENT_DIR))


@dataclass
class Image:
    id: int
    url: str
    orig: str
    is_flatplan: bool


ImageSchema = marshmallow_dataclass.class_schema(Image)()

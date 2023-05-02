from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Optional

import marshmallow_dataclass

CURRENT_DIR = Path(__file__).parent.absolute()
sys.path.append(str(CURRENT_DIR))


@dataclass
class User:
    id: int
    phone: Optional[str]
    name: str
    joined: str
    has_email: bool
    verified: bool
    type_new: Optional[bool]


UserSchema = marshmallow_dataclass.class_schema(User)()

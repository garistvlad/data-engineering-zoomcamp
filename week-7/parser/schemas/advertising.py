from dataclasses import dataclass
from pathlib import Path
import sys
from typing import List, Dict, Any, Optional

import marshmallow_dataclass

CURRENT_DIR = Path(__file__).parent.absolute()
sys.path.append(str(CURRENT_DIR))

from user import User
from image import Image
from coordinates import Coordinates


@dataclass
class CloudinaryVideo:
    id: Optional[str]
    format: Optional[str]
    duration: Optional[float]


@dataclass
class Advertising:
    id: int
    title: str
    slug: str
    rubric: int
    description: str
    city: int
    city_districts: List[int]
    user: User
    images: List[Image]
    attrs: Dict
    price: float
    hit_count: int
    phone_hitcount: int
    currency: str
    created_dt: str
    raise_dt: str
    owner_advert_count: int
    phone_hide: bool
    coordinates: Optional[Coordinates]
    zoom: Optional[int]
    negotiable_price: bool
    exchange: bool
    imei_checked: bool
    price_description: Optional[str]
    in_top: bool
    in_premium: bool
    is_editable: bool
    is_favorite: bool
    video_link: Optional[str]
    cloudinary_video: Optional[CloudinaryVideo]
    all_images: List[str]
    templated_title: str
    credit_type: Optional[str]
    credit_attrs: Optional[str]
    credit_link: Optional[str]
    flatplan: bool
    virtual_tour_link: Optional[str]
    is_carcheck: bool
    new_in_stock_label: bool
    new_to_order_label: bool
    price_from: bool
    price_short: Optional[str]


AdvertisingSchema = marshmallow_dataclass.class_schema(Advertising)()

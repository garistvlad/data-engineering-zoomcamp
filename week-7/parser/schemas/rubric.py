from dataclasses import dataclass
from typing import List, Optional

import marshmallow_dataclass


@dataclass
class RubricFeatureChoiceOption:
    key: str
    value: str


@dataclass
class RubricFeature:
    feature_name: str
    feature_verbose_name: str
    feature_type: str
    feature_type_id: int
    feature_choices: List[RubricFeatureChoiceOption]
    filter_feature: bool
    required: bool
    measure_unit: Optional[str]


@dataclass
class Rubric:
    id: int
    name: str
    slug: str
    path: str
    has_children: bool
    rubric_features: List[RubricFeature]
    img: Optional[str]
    is_job_rubric: bool
    is_escort_rubric: bool
    auto_title: bool
    is_adult_rubric: bool
    is_animals_rubric: bool
    is_virtual_tour_rubric: bool
    is_realestate_rubric: bool
    is_iphone_rubric: bool
    exchange_possible: bool
    price_required: bool
    can_phone_hide: bool
    absolute_url: str
    adv_count: int
    parent_id: Optional[int]
    flatplan_enabled: bool
    posting_allowed_error: Optional[str]
    breadcrumbs: List[str]
    rubric_type: int
    rubric_type_slug: str
    is_delivery_rubric:	bool
    is_shop_delivery_rubric: bool
    auction_enabled: bool
    cloudinary_video_enabled: bool
    imei_check_enabled: bool
    is_carcheck: bool
    is_service_rubric: bool = False


# Marshmallow Schema:
RubricSchema = marshmallow_dataclass.class_schema(Rubric)()
RubricFeatureSchema = marshmallow_dataclass.class_schema(RubricFeature)()

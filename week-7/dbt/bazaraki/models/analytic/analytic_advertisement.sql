{{ config(
    materialized='table',
    partition_by={
        "field": "created_time",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=["city_name", "rubric_name"],
) }}

select
  ad.advertisement_id,
  case
    when rubric.url like 'real-estate-to-rent%' then 'rent'
    when rubric.url like 'real-estate-for-sale%' then 'sale'
    else 'other'
  end as advertisement_type,
  ad.created_dt as created_time,
  ad.raise_dt as published_time,
  ad.attrs,
  ad.title,
  ad.description,
  ad.price,
  ad.hit_count,
  --
  ad.city_id,
  city.name as city_name,
  --
  ad.district_id,
  district.name as district_name,
  --
  ad.rubric_id,
  rubric.name as rubric_name,
  rubric.url as rubric_url,
  --
  ad.user_id,
  user.name as user_name,
  user.registration_date as user_registration_date

from {{ ref('core_advertisement') }} ad
join {{ ref('stg_city') }} city
  on ad.city_id = city.city_id
join {{ ref('stg_rubric') }} rubric
  on ad.rubric_id = rubric.rubric_id
join {{ ref('core_user') }} user
  on user.user_id = ad.user_id
join {{ ref('stg_district') }} district
  on district.district_id = ad.district_id

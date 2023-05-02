{{ config(materialized='table') }}

select
    district_id,
    name,
    slug,
    city_id,
    post_codes,
    coordinates
from
    {{ source('bazaraki', 'external_district') }} district

{{ config(materialized='table') }}

select
    city_id,
    name,
    slug,
    coordinates
from
    {{ source('bazaraki', 'external_city') }}

{{ config(materialized='table') }}

select
    image_id,
    advertisement_id
from
    {{ source('bazaraki', 'external_image_to_advertisement_mapping_incremental') }}

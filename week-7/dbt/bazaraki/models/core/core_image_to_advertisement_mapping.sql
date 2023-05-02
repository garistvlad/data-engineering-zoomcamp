{{ config(materialized='view') }}

select
    image_id,
    advertisement_id
from {{ ref('stg_image_to_advertisement_mapping_incremental') }}

union distinct

select
    image_id,
    advertisement_id
from {{ ref('stg_image_to_advertisement_mapping_full') }}

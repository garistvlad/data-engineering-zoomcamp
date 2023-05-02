{{ config(materialized='table') }}

select
    image_id,
    compressed_url,
    origiral_url,
    is_flatplan
from
    {{ source('bazaraki', 'external_image_incremental') }}

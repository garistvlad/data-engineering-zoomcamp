{{ config(materialized='view') }}

select
    image_id,
    compressed_url,
    origiral_url,
    is_flatplan

from {{ ref('stg_image_incremental') }}

union all

select
    image_id,
    compressed_url,
    origiral_url,
    is_flatplan

from {{ ref('stg_image_full') }}
where true
  -- not presented in incremental data
  and image_id not in (
    select image_id
    from {{ ref('stg_image_incremental') }}
  )

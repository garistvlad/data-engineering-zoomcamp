{{ config(materialized='view') }}

select
    user_id,
    phone,
    name,
    registration_date,
    has_email,
    verified,
    type_new
from {{ ref('stg_user_incremental') }}

union all

select
    user_id,
    phone,
    name,
    registration_date,
    has_email,
    verified,
    type_new
from {{ ref('stg_user_full') }}
where true
  -- not presented in incremental data
  and user_id not in (
    select user_id
    from {{ ref('stg_user_incremental') }}
  )

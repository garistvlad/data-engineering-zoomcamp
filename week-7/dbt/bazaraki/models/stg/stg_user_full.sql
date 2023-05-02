{{ config(materialized='table') }}

select
  user_id,
  phone,
  name,
  registration_date,
  has_email,
  verified,
  type_new
from
    {{ source('bazaraki', 'external_user_full') }}

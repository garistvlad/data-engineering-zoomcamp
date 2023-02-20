{{ config(materialized='view') }}

select
  dispatching_base_num as dispatching_base_number,
  cast(pulocationid as integer) as pickup_location_id,
  cast(dolocationid as integer) as dropoff_location_id,

  cast(pickup_datetime as timestamp) as pickup_datetime,
  cast(dropoff_datetime as timestamp) as dropoff_datetime,

  sr_flag,
  affiliated_base_number

from {{ source('staging', 'fhv_trips') }}

-- dbt build -s stg_fhv_trips --var 'is_test_run: true'
{% if var('is_test_run', default=False) %}
limit 100
{% endif %}
{{ config(materialized='view') }}

select
  -- identifiers
  {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as trip_id,
  cast(vendorid as integer) as vendor_id,
  cast(ratecodeid as integer) as ratecode_id,
  cast(pulocationid as integer) as pickup_location_id,
  cast(dolocationid as integer) as dropoff_location_id,

  -- timestamps
  cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
  cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

  -- trip info
  store_and_fwd_flag,
  cast(trip_type as integer) as trip_type,
  cast(trip_distance as numeric) as trip_distance,
  cast(passenger_count as integer) as passender_count,

  -- payment info
  cast(payment_type as integer) as payment_type,
  {{ get_payment_type_description('payment_type') }} as payment_type_description,
  cast(fare_amount as numeric) as fare_amount,
  cast(extra as numeric) as extra,
  cast(mta_tax as numeric) as mta_tax,
  cast(tip_amount as numeric) as tip_amount,
  cast(tolls_amount as numeric) as tolls_amount,
  cast(ehail_fee as numeric) as ehail_fee,
  cast(improvement_surcharge as numeric) as improvement_surcharge,
  cast(congestion_surcharge as numeric) as congestion_surcharge,
  cast(total_amount as numeric) as total_amount

from {{ source('staging', 'green_trips') }}

-- dbt build -s stg_green_trips --var 'is_test_run: true'
{% if var('is_test_run', default=False) %}
limit 100
{% endif %}
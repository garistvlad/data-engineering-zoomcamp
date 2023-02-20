{{
    config(
        materialized='table',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",
            "granularity": "day"
        })
}}


with green_trips as (
    select *,
           'Green' as service_type
    from {{ ref('stg_green_trips') }}
),

yellow_trips as (
    select *,
           'Yellow' as service_type
    from {{ ref('stg_yellow_trips') }}
),

all_trips as (
    select * from green_trips
    union all
    select * from yellow_trips
),

dim_zones as (
    select *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
  all_trips.service_type,
  -- identifiers
  all_trips.trip_id,
  all_trips.vendor_id,
  all_trips.ratecode_id,

  all_trips.pickup_location_id,
  pickup_z.zone as pickup_zone,
  pickup_z.borough as pickup_borough,

  all_trips.dropoff_location_id,
  dropoff_z.zone as dropoff_zone,
  dropoff_z.borough as dropoff_borough,

  -- timestamps
  all_trips.pickup_datetime,
  all_trips.dropoff_datetime,

  -- trip info
  all_trips.store_and_fwd_flag,
  all_trips.trip_type,
  all_trips.trip_distance,
  all_trips.passender_count,

  -- payment info
  all_trips.payment_type,
  all_trips.payment_type_description,
  all_trips.fare_amount,
  all_trips.extra,
  all_trips.mta_tax,
  all_trips.tip_amount,
  all_trips.tolls_amount,
  all_trips.ehail_fee,
  all_trips.improvement_surcharge,
  all_trips.congestion_surcharge,
  all_trips.total_amount

from all_trips
join dim_zones as pickup_z
    on all_trips.pickup_location_id = pickup_z.location_id
join dim_zones as dropoff_z
    on all_trips.dropoff_location_id = dropoff_z.location_id

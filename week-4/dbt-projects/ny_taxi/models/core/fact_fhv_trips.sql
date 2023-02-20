{{
    config(
        materialized='table',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",
            "granularity": "day"
        })
}}


with fhv_trips as (
    select *
    from {{ ref('stg_fhv_trips') }}
    where true
        and pickup_location_id is not null
        and dropoff_location_id is not null
),

dim_zones as (
    select *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
  fhv_trips.dispatching_base_number,
  fhv_trips.affiliated_base_number,
  fhv_trips.sr_flag,
  -- timestamps
  fhv_trips.pickup_datetime,
  fhv_trips.dropoff_datetime,
  -- pickup
  fhv_trips.pickup_location_id,
  pickup_z.zone as pickup_zone,
  pickup_z.borough as pickup_borough,
  -- drop off
  fhv_trips.dropoff_location_id,
  dropoff_z.zone as dropoff_zone,
  dropoff_z.borough as dropoff_borough

from fhv_trips
join dim_zones as pickup_z
    on fhv_trips.pickup_location_id = pickup_z.location_id
join dim_zones as dropoff_z
    on fhv_trips.dropoff_location_id = dropoff_z.location_id

{{ config(materialized='table') }}

with all_trips as (

    select *, 'yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}

    union all

    select *, 'green' as service_type
    from {{ ref('stg_green_tripdata') }}

),

filtered_trips as (

    select *
    from all_trips
    where pickup_locationid is not null
      and dropoff_locationid is not null

)

select
    row_number() over () as trip_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_locationid,
    dropoff_locationid,
    total_amount,
    passenger_count,
    service_type
from filtered_trips

{{ config(materialized='table') }}

with base as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid    as pickup_location_id,
        dolocationid    as dropoff_location_id,
        sr_flag,
        hvfhs_license_num,
        ratecodeid,
        passenger_count,
        trip_distance,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    from {{ source('nyc_taxi', 'fhv_tripdata_2019') }}

    where dispatching_base_num is not null

)

select * from base

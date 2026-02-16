{{ config(materialized='table') }}

with revenue as (
    select
        date_trunc('month', pickup_datetime) as month,
        pickup_locationid as zone_id,
        sum(total_amount) as total_revenue,
        count(*) as trip_count
    from {{ ref('fact_trips') }}
    group by 1, 2
)

select
    r.month,
    zn.zone,
    zn.borough,
    r.total_revenue,
    r.trip_count
from revenue r
join {{ ref('dim_zones') }} zn
    on r.zone_id = zn.locationid

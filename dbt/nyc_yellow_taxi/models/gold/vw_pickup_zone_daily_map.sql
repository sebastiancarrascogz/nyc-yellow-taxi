{{ config(
    materialized='view'
) }}

select
    a.*,
    d.zone_name as pu_zone_name,
    d.geometry

from {{ ref('agg_pickup_zone_daily') }} a

left join {{ ref('dim_taxi_zones') }} d
    on a.pu_location_id = d.location_id
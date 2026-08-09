{{config(materialized='table')}}

with pickup_zone_daily as (
    select 
pu_location_id, -- Campo relevante para el mapa
count(*) as trip_count, -- Campo relevante para el mapa
date(tpep_pickup_datetime) as pickup_date,
pu_borough_name,
do_borough_name,
trip_distance_categ,
payment_type_name,
passenger_count as trip_passenger_count
from {{ref('fct_taxi_trips')}} 
group by pickup_date, 
pu_borough_name,
pu_location_id, 
do_borough_name,
trip_distance_categ,
payment_type_name,
trip_passenger_count
)
select 
p.*,
d.zone_name as pu_zone_name,
d.geometry
from pickup_zone_daily p 
left join {{ref('dim_taxi_zones')}} d on p.pu_location_id = d.location_id
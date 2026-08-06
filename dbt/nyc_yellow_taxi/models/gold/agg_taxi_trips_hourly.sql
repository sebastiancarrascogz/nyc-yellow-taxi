{{config(materialized='table')}}

select 
extract(hour from tpep_pickup_datetime) as pickup_hour,
count(*) as trip_count,
date(tpep_pickup_datetime) as pickup_date,
pu_borough_name,
do_borough_name,
trip_distance_categ,
payment_type_name,
passenger_count as trip_passenger_count
from {{ref('fct_taxi_trips')}}
group by pickup_hour, 
pickup_date, 
pu_borough_name, 
do_borough_name,
trip_distance_categ,
payment_type_name,
trip_passenger_count
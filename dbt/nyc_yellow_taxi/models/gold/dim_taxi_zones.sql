{{ config(materialized="table")}}

select 
location_id,
zone_name,
borough_name,
geometry
from {{ref("taxi_zones")}}


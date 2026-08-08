{{ config(materialized='table') }}

select 
LocationID as location_id,
zone as zone_name,
borough as borough_name,
geometry
from {{source('bronze', 'taxi_zones')}}

{{config(materialized='table')}}

select 
from {{ref('fct_taxi_trips')}}
where 
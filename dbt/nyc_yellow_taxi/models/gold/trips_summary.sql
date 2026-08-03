{{ config(materialized='table') }}

select 
trip_id,
tpep_pickup_datetime,
tpep_dropoff_datetime,
passenger_count,
trip_distance, -- Categorizar
ratecode_name,
pu_borough_name,
pu_borough_name,
payment_type_name,
extra, -- booleano este
case when extra > 0 then True else False end as has_extra, 
tip_amount, -- categorizado
total_amount, -- categ
congestion_surcharge, --bool
case when congestion_surcharge > 0 then True else False end as has_congestion_surcharge, 
airport_fee --bool
case when airport_fee > 0 then True else False end as has_airport_fee
from {{ref('yellow_trips')}}
where not is_extra_negative
and not is_tip_negative
and not is_total_amount_negative
and not is_congestion_surcharge_negative
and not is_airport_fee_negative 
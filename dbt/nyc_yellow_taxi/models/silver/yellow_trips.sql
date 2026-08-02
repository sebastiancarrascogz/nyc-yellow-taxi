{{ config(materialized='table') }}

select 
TO_HEX(MD5(CONCAT(CAST(yt.vendor_id AS STRING),
    CAST(yt.tpep_pickup_datetime AS STRING),
    CAST(yt.tpep_dropoff_datetime AS STRING),
    CAST(yt.pu_location_id AS STRING),
    CAST(yt.do_location_id AS STRING)
))) as trip_id, -- 1. Crear PK
vi.name as vendor_name,
yt.tpep_pickup_datetime,
yt.tpep_dropoff_datetime,
yt.passenger_count,         
yt.trip_distance,           
ri.name as ratecode_name,              
yt.store_and_fwd_flag,      
pu_zones.zone as pu_location_name,            
do_zones.zone as do_location_name,
pt.name as payment_type_name,       
yt.fare_amount,     
case when yt.fare_amount < 0 then True else False end as is_fare_negative, -- .5             
yt.extra,                   
case when yt.extra < 0 then True else False end as is_extra_negative, -- .6             
yt.mta_tax,                 
case when yt.mta_tax < 0 then True else False end as is_mta_tax_negative, -- .7
yt.tip_amount,
case when yt.tip_amount < 0 then True else False end as is_tip_negative, -- .8              
yt.tolls_amount,            
case when yt.tolls_amount < 0 then True else False end as is_tolls_negative, -- .9
yt.improvement_surcharge,   
case when yt.improvement_surcharge < 0 then True else False end as is_improvement_surcharge_negative, -- .10
yt.total_amount,            
case when yt.total_amount < 0 then True else False end as is_total_amount_negative, -- .11
yt.congestion_surcharge,    
case when yt.congestion_surcharge < 0 then True else False end as is_congestion_surcharge_negative, -- .12
yt.airport_fee,             
case when yt.airport_fee < 0 then True else False end as is_airport_fee_negative -- .13
from {{source('bronze', 'yellow_trips')}} yt
left join {{ref('vendor_id')}} vi on yt.vendor_id = vi.id
left join {{ref('payment_type')}} pt on yt.payment_type = pt.id
left join {{ref('ratecode_id')}} ri on coalesce(yt.ratecode_id, 99) = ri.id -- 4. Imputar nulos a 99
left join {{ref('taxi_zone_lookup')}} pu_zones on yt.pu_location_id = pu_zones.location_id
left join {{ref('taxi_zone_lookup')}} do_zones on yt.do_location_id = do_zones.location_id
where yt.tpep_pickup_datetime < yt.tpep_dropoff_datetime -- 1. Inconsistencia de fechas
and yt.tpep_pickup_datetime >= '2009-01-01' and yt.tpep_dropoff_datetime >= '2009-01-01'
and  yt.tpep_pickup_datetime <= current_timestamp() and yt.tpep_dropoff_datetime <= current_timestamp()
and yt.passenger_count >= 1 and yt.passenger_count < 6 -- 2. Pasajeros
and yt.trip_distance > 0 and yt.trip_distance < 30 -- 3. Filtro de distancia 
and yt.fare_amount >= -140 and yt.fare_amount <= 140 and yt.fare_amount <> 0 -- .5
and yt.extra >= -15 and yt.extra <= 15 -- .6
and yt.mta_tax >= -{{var('mta_tax_max')}} and yt.mta_tax <= {{var('mta_tax_max')}} -- .7 (Parametrizado)
and yt.tip_amount >= -100 and yt.tip_amount <= 100 -- .8
and yt.tolls_amount >= -35 and yt.tolls_amount <= 35 -- .9
and yt.improvement_surcharge >= -{{var('improvement_surcharge_max')}} and yt.improvement_surcharge <= {{var('improvement_surcharge_max')}} -- .10
and yt.total_amount >= -200 and yt.total_amount <= 200 and yt.total_amount <> 0 -- .11
and coalesce(yt.congestion_surcharge, 0) >= -{{var('congestion_surcharge_max')}} and coalesce(yt.congestion_surcharge, 0) <= {{var('congestion_surcharge_max')}} -- .12
and coalesce(yt.airport_fee, 0) >= -{{var('airport_fee_max')}} and coalesce(yt.airport_fee, 0) <= {{var('airport_fee_max')}} -- .13
QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY tpep_pickup_datetime) = 1 -- Unicidad 
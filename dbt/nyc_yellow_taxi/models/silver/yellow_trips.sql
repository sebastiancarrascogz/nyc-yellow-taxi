{{ config(materialized='table') }}

select 
TO_HEX(MD5(CONCAT(CAST(yt.vendor_id AS STRING),
    CAST(yt.tpep_pickup_datetime AS STRING),
    CAST(yt.tpep_dropoff_datetime AS STRING),
    CAST(yt.pu_location_id AS STRING),
    CAST(yt.do_location_id AS STRING)
))) as trip_id, 
vi.name as vendor_name,
datetime(yt.tpep_pickup_datetime, 'America/New_York') as tpep_pickup_datetime,
datetime(yt.tpep_dropoff_datetime,'America/New_York') as tpep_dropoff_datetime,
timestamp_diff(yt.tpep_dropoff_datetime, yt.tpep_pickup_datetime, second)/60.0 as trip_duration_minutes,
cast(yt.passenger_count as int64) as passenger_count,         
yt.trip_distance,           
ri.name as ratecode_name,              
yt.store_and_fwd_flag,      
yt.pu_location_id,
pu_zones.zone as pu_location_name,
yt.do_location_id,
do_zones.zone as do_location_name,
pu_zones.borough as pu_borough_name,
case when do_zones.borough = "N/A" then "Outside of NYC" else do_zones.borough end as do_borough_name,
pt.name as payment_type_name,       
yt.fare_amount,     
case when yt.fare_amount < 0 then True else False end as is_fare_negative,          
yt.extra,                   
case when yt.extra < 0 then True else False end as is_extra_negative,            
yt.mta_tax,                 
case when yt.mta_tax < 0 then True else False end as is_mta_tax_negative, 
yt.tip_amount,
case when yt.tip_amount < 0 then True else False end as is_tip_negative,              
yt.tolls_amount,            
case when yt.tolls_amount < 0 then True else False end as is_tolls_negative, 
yt.improvement_surcharge,   
case when yt.improvement_surcharge < 0 then True else False end as is_improvement_surcharge_negative,
yt.total_amount,            
case when yt.total_amount < 0 then True else False end as is_total_amount_negative, 
yt.congestion_surcharge,    
case when yt.congestion_surcharge < 0 then True else False end as is_congestion_surcharge_negative, 
yt.airport_fee,             
case when yt.airport_fee < 0 then True else False end as is_airport_fee_negative 
from {{source('bronze', 'yellow_trips')}} yt
left join {{ref('vendor_id')}} vi on yt.vendor_id = vi.id
left join {{ref('payment_type')}} pt on yt.payment_type = pt.id
left join {{ref('ratecode_id')}} ri on coalesce(yt.ratecode_id, 99) = ri.id 
left join {{ref('taxi_zone_lookup')}} pu_zones on yt.pu_location_id = pu_zones.location_id
left join {{ref('taxi_zone_lookup')}} do_zones on yt.do_location_id = do_zones.location_id
where yt.pu_location_id not in (264,265) and yt.do_location_id <> 264 
and datetime(yt.tpep_pickup_datetime, 'America/New_York') >= '2009-01-01' and datetime(yt.tpep_dropoff_datetime, 'America/New_York') >= '2009-01-01'
and  yt.tpep_pickup_datetime <= current_timestamp() and yt.tpep_dropoff_datetime <= current_timestamp()
and timestamp_diff(yt.tpep_dropoff_datetime, yt.tpep_pickup_datetime, second) > 0 -- Filtra DO < PU 
and timestamp_diff(yt.tpep_dropoff_datetime,yt.tpep_pickup_datetime, second) <= {{ var('silver')['trip_duration_max'] }} * 60
and yt.passenger_count >= 1 and yt.passenger_count < 6 
and yt.trip_distance > 0 and yt.trip_distance < 30 
and yt.fare_amount >= -140 and yt.fare_amount <= 140 and yt.fare_amount <> 0 
and yt.extra >= -15 and yt.extra <= 15 
and yt.mta_tax >= -{{var('silver')['mta_tax_max']}} and yt.mta_tax <= {{var('silver')['mta_tax_max']}} 
and yt.tip_amount >= -100 and yt.tip_amount <= 100 
and yt.tolls_amount >= -35 and yt.tolls_amount <= 35 
and yt.improvement_surcharge >= -{{var('silver')['improvement_surcharge_max']}} and yt.improvement_surcharge <= {{var('silver')['improvement_surcharge_max']}} 
and yt.total_amount >= -200 and yt.total_amount <= 200 and yt.total_amount <> 0 
and coalesce(yt.congestion_surcharge, 0) >= -{{var('silver')['congestion_surcharge_max']}} and coalesce(yt.congestion_surcharge, 0) <= {{var('silver')['congestion_surcharge_max']}} 
and coalesce(yt.airport_fee, 0) >= -{{var('silver')['airport_fee_max']}} and coalesce(yt.airport_fee, 0) <= {{var('silver')['airport_fee_max']}} 
QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY tpep_pickup_datetime) = 1 -- unicidad 
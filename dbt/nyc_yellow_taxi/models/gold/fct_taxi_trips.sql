{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_id',
    partition_by={
        "field": "pickup_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by='trip_id'
) }}

select 
trip_id,
vendor_name,
fare_amount,
tpep_pickup_datetime,
tpep_dropoff_datetime,
round(trip_duration_minutes, 2) as trip_duration_minutes,
passenger_count,
trip_distance,
case 
    when trip_distance <= {{var('gold')['trip_distance']['short_max']}} then concat('Corto (≤',cast({{var('gold')['trip_distance']['short_max']}} as string),' mi)')
    when trip_distance <= {{var('gold')['trip_distance']['medium_max']}} then concat('Medio (',cast({{var('gold')['trip_distance']['short_max']}} as string), '-', cast({{var('gold')['trip_distance']['medium_max']}} as string),' mi)')
    when trip_distance <= {{var('gold')['trip_distance']['long_max']}} then concat('Largo (',cast({{var('gold')['trip_distance']['medium_max']}} as string), '-', cast({{var('gold')['trip_distance']['long_max']}} as string),' mi)')
    else concat('Muy Largo (≥',cast({{var('gold')['trip_distance']['long_max']}} as string),' mi)') end as trip_distance_categ,
ratecode_name,
pu_location_id,
do_location_id,
pu_borough_name,
do_borough_name,
case 
    when payment_type_name = 'Cash' then 'Efectivo' 
    when payment_type_name = 'Credit card' then 'Tarjeta de crédito' else 'Otro' end as payment_type_name,
extra,
extra > 0 as has_extra, 
tip_amount,
case 
    when tip_amount = 0 then 'no_tip'
    when tip_amount <= {{var('gold')['tip_amount']['low_max']}} then 'low'
    when tip_amount <= {{var('gold')['tip_amount']['medium_max']}} then 'medium'
    when tip_amount <= {{var('gold')['tip_amount']['high_max']}} then 'high'
    else 'very high' end as tip_amount_categ,
tip_amount > 0 as has_tip,
total_amount, 
case 
    when total_amount <= {{var('gold')['total_amount']['low_max']}} then 'low'
    when total_amount <= {{var('gold')['total_amount']['medium_max']}} then 'medium'
    when total_amount <= {{var('gold')['total_amount']['high_max']}} then 'high'
    else 'very high' end as total_amount_categ,
congestion_surcharge, 
congestion_surcharge > 0 as has_congestion_surcharge, 
airport_fee,
airport_fee > 0 as has_airport_fee,
date(tpep_pickup_datetime) as pickup_date,
source_file_month,
ingested_at
from {{ref('yellow_trips')}}
where payment_type_name not in ('Dispute', 'No charge') 
and pu_borough_name <> 'EWR' -- zona fuera de NYC 
and not is_extra_negative 
and not is_tip_negative 
and not is_total_amount_negative 
and not is_fare_negative
and not is_congestion_surcharge_negative 
and not is_airport_fee_negative 
{% if is_incremental() %}
and source_file_month = date('{{ var("batch_month") }}')
{% endif %}
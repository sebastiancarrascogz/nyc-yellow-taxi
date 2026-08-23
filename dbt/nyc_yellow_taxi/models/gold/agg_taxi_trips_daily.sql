-- depends_on: {{ ref('yellow_trips') }}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "pickup_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

with 
{% if is_incremental() %}
affected_dates as (
    select distinct
        date(tpep_pickup_datetime) as pickup_date
    from {{ ref('yellow_trips') }}
    where source_file_month = date('{{ var("batch_month") }}')
),
{% endif %}

agg_daily as (
    select 
pickup_date,
round(sum(trip_duration_minutes), 2) as trip_duration_minutes_sum,
pu_borough_name,
do_borough_name,
trip_distance_categ,
payment_type_name,
passenger_count as trip_passenger_count,
sum(passenger_count) as total_passengers,
count(*) as trip_count,
round(sum(total_amount), 2) as total_amount_sum,
round(sum(trip_distance), 2) as trip_distance_sum,
round(sum(tip_amount), 2) as tip_amount_sum,
countif(has_congestion_surcharge is true and has_airport_fee is not true) as congestion_surcharge_only_trip_count,
countif(has_airport_fee is true and has_congestion_surcharge is not true) as airport_fee_only_trip_count,
countif(has_congestion_surcharge is true and has_airport_fee is true) as both_special_charges_trip_count,
countif(has_congestion_surcharge is not true and has_airport_fee is not true) as no_special_charges_trip_count,
sum(congestion_surcharge) as congestion_surcharge_sum,
sum(airport_fee) as airport_fee_sum
from {{ref('fct_taxi_trips')}}
{% if is_incremental() %}
where pickup_date in (select pickup_date from affected_dates)
{% endif %}
group by pickup_date, 
pu_borough_name, 
do_borough_name,
trip_distance_categ,
payment_type_name,
trip_passenger_count
)
select 
*,
cast(format_date('%u', pickup_date) as INT64) as pickup_weekday_num,
case cast(format_date('%u', pickup_date) as INT64)
    when 1 then 'Lunes'
    when 2 then 'Martes'
    when 3 then 'Miércoles'
    when 4 then 'Jueves'
    when 5 then 'Viernes'
    when 6 then 'Sábado'
    when 7 then 'Domingo'
end as pickup_weekday_name
from agg_daily

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
select distinct date(tpep_pickup_datetime) as pickup_date
from {{ ref('yellow_trips') }}
where source_file_month = date('{{ var("batch_month") }}')
),

{% endif %}

pickup_zone_daily as (
select
pu_location_id,
count(*) as trip_count,
pickup_date,
pu_borough_name,
do_borough_name,
trip_distance_categ,
payment_type_name,
passenger_count as trip_passenger_count
from {{ ref('fct_taxi_trips') }}

{% if is_incremental() %}
where pickup_date in (select pickup_date from affected_dates)
{% endif %}

group by
pickup_date,
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
left join {{ ref('dim_taxi_zones') }} d on p.pu_location_id = d.location_id
with affected_dates as (
    select distinct
        date(tpep_pickup_datetime) as pickup_date
    from {{ ref('yellow_trips') }}
    where source_file_month = date('{{ var("batch_month") }}')
),
fact as (
    select
        count(*) as trip_count
    from {{ ref('fct_taxi_trips') }}
    where pickup_date in (
        select pickup_date
        from affected_dates
    )
),
agg as (
    select
        sum(trip_count) as trip_count
    from {{ ref('agg_taxi_trips_daily') }}
    where pickup_date in (
        select pickup_date
        from affected_dates
    )
)

select
    f.trip_count as fact_trip_count,
    a.trip_count as agg_trip_count
from fact f
cross join agg a
where f.trip_count != a.trip_count
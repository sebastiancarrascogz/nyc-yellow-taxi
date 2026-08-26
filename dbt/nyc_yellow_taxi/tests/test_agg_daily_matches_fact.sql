with fact as (
select
count(*) as trip_count
from {{ ref('fct_taxi_trips') }}
),
agg as (
select
sum(trip_count) as trip_count
from {{ ref('agg_taxi_trips_daily') }}
)
select
f.trip_count as fact_trip_count,
a.trip_count as agg_trip_count
from fact f
cross join agg a
where f.trip_count != a.trip_count
-- Custom Query a partir de Gold para construir la serving layer que consume el Dashboard

WITH params AS (
    SELECT
        PARSE_DATE('%Y%m%d', @DS_START_DATE) AS start_date,
        PARSE_DATE('%Y%m%d', @DS_END_DATE) AS end_date
),

-- ============================================================
-- DATE SPINE
-- Una fila por cada fecha del período, aunque no haya viajes
-- ============================================================

date_spine AS (
    SELECT
        pickup_date
    FROM params p
    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(p.start_date, p.end_date)
    ) AS pickup_date
),

-- ============================================================
-- DAILY
-- Rama analítica principal
-- ============================================================

daily AS (
    SELECT
        d.pickup_date,
        d.pickup_weekday_num,
        d.pickup_weekday_name,
        d.payment_type_name,
        d.pu_borough_name,
        d.trip_distance_categ,
        d.trip_passenger_count,

        SUM(d.trip_count) AS trip_count,
        SUM(d.trip_duration_minutes_sum) AS trip_duration_minutes_sum,
        SUM(d.total_passengers) AS total_passengers,
        SUM(d.total_amount_sum) AS total_amount_sum,
        SUM(d.trip_distance_sum) AS trip_distance_sum,
        SUM(d.tip_amount_sum) AS tip_amount_sum,

        SUM(d.congestion_surcharge_only_trip_count)
            AS congestion_surcharge_only_trip_count,

        SUM(d.airport_fee_only_trip_count)
            AS airport_fee_only_trip_count,

        SUM(d.both_special_charges_trip_count)
            AS both_special_charges_trip_count,

        SUM(d.no_special_charges_trip_count)
            AS no_special_charges_trip_count,

        SUM(d.congestion_surcharge_sum)
            AS congestion_surcharge_sum,

        SUM(d.airport_fee_sum)
            AS airport_fee_sum

    FROM `personalproject-463600.nyc_taxi_gold.agg_taxi_trips_daily` d

    CROSS JOIN params p

    WHERE d.pickup_date BETWEEN p.start_date AND p.end_date
      AND d.payment_type_name IN UNNEST(@p_payment_type)
      AND d.pu_borough_name IN UNNEST(@p_pu_borough)
      AND d.do_borough_name IN UNNEST(@p_do_borough)
      AND d.trip_distance_categ IN UNNEST(@p_distance)
      AND d.trip_passenger_count IN UNNEST(@p_passengers)

    GROUP BY
        d.pickup_date,
        d.pickup_weekday_num,
        d.pickup_weekday_name,
        d.payment_type_name,
        d.pu_borough_name,
        d.trip_distance_categ,
        d.trip_passenger_count
),

-- ============================================================
-- DAILY SERIES
-- Total diario + fechas sin viajes rellenadas con cero
-- ============================================================

daily_totals AS (
    SELECT
        pickup_date,
        SUM(trip_count) AS trip_count
    FROM daily
    GROUP BY pickup_date
),

daily_series AS (
    SELECT
        s.pickup_date,

        CAST(FORMAT_DATE('%u', s.pickup_date) AS INT64)
            AS pickup_weekday_num,

        CASE CAST(FORMAT_DATE('%u', s.pickup_date) AS INT64)
            WHEN 1 THEN 'Lunes'
            WHEN 2 THEN 'Martes'
            WHEN 3 THEN 'Miércoles'
            WHEN 4 THEN 'Jueves'
            WHEN 5 THEN 'Viernes'
            WHEN 6 THEN 'Sábado'
            WHEN 7 THEN 'Domingo'
        END AS pickup_weekday_name,

        COALESCE(d.trip_count, 0) AS trip_count

    FROM date_spine s

    LEFT JOIN daily_totals d
        ON s.pickup_date = d.pickup_date
),

-- ============================================================
-- HOURLY
-- Fecha × 24 horas, rellenando horas sin viajes con cero
-- ============================================================

date_hour_spine AS (
    SELECT
        d.pickup_date,
        pickup_hour
    FROM date_spine d

    CROSS JOIN UNNEST(
        GENERATE_ARRAY(0, 23)
    ) AS pickup_hour
),

hourly_filtered AS (
    SELECT
        h.pickup_date,
        h.pickup_hour,
        SUM(h.trip_count) AS trip_count

    FROM `personalproject-463600.nyc_taxi_gold.agg_taxi_trips_hourly` h

    CROSS JOIN params p

    WHERE h.pickup_date BETWEEN p.start_date AND p.end_date
      AND h.payment_type_name IN UNNEST(@p_payment_type)
      AND h.pu_borough_name IN UNNEST(@p_pu_borough)
      AND h.do_borough_name IN UNNEST(@p_do_borough)
      AND h.trip_distance_categ IN UNNEST(@p_distance)
      AND h.trip_passenger_count IN UNNEST(@p_passengers)

    GROUP BY
        h.pickup_date,
        h.pickup_hour
),

hourly AS (
    SELECT
        s.pickup_date,
        s.pickup_hour,
        COALESCE(h.trip_count, 0) AS trip_count

    FROM date_hour_spine s

    LEFT JOIN hourly_filtered h
        ON s.pickup_date = h.pickup_date
       AND s.pickup_hour = h.pickup_hour
),

-- ============================================================
-- MAP
-- ============================================================

map_metrics AS (
    SELECT
        m.pu_location_id,
        SUM(m.trip_count) AS trip_count

    FROM `personalproject-463600.nyc_taxi_gold.agg_pickup_zone_daily` m

    CROSS JOIN params p

    WHERE m.pickup_date BETWEEN p.start_date AND p.end_date
      AND m.payment_type_name IN UNNEST(@p_payment_type)
      AND m.pu_borough_name IN UNNEST(@p_pu_borough)
      AND m.do_borough_name IN UNNEST(@p_do_borough)
      AND m.trip_distance_categ IN UNNEST(@p_distance)
      AND m.trip_passenger_count IN UNNEST(@p_passengers)

    GROUP BY
        m.pu_location_id
),

map AS (
    SELECT
        z.location_id AS pu_location_id,
        z.zone_name AS pu_zone_name,
        z.borough_name AS pu_zone_borough_name,
        z.geometry,
        COALESCE(m.trip_count, 0) AS trip_count

    FROM `personalproject-463600.nyc_taxi_gold.dim_taxi_zones` z

    LEFT JOIN map_metrics m
        ON z.location_id = m.pu_location_id

    WHERE z.borough_name IN UNNEST(@p_pu_borough)
)

-- ============================================================
-- DAILY
-- ============================================================

SELECT
    'daily' AS row_type,

    d.pickup_date AS date_anchor,
    d.pickup_date,
    d.pickup_weekday_num,
    d.pickup_weekday_name,
    d.payment_type_name,
    d.pu_borough_name,
    d.trip_distance_categ,
    d.trip_passenger_count,

    CAST(NULL AS INT64) AS pickup_hour,

    CAST(NULL AS INT64) AS pu_location_id,
    CAST(NULL AS STRING) AS pu_zone_name,
    CAST(NULL AS STRING) AS pu_zone_borough_name,
    CAST(NULL AS GEOGRAPHY) AS geometry,

    d.trip_count,
    d.trip_duration_minutes_sum,
    d.total_passengers,
    d.total_amount_sum,
    d.trip_distance_sum,
    d.tip_amount_sum,

    d.congestion_surcharge_only_trip_count,
    d.airport_fee_only_trip_count,
    d.both_special_charges_trip_count,
    d.no_special_charges_trip_count,
    d.congestion_surcharge_sum,
    d.airport_fee_sum

FROM daily d

UNION ALL

-- ============================================================
-- DAILY SERIES
-- ============================================================

SELECT
    'daily_series' AS row_type,

    d.pickup_date AS date_anchor,
    d.pickup_date,
    d.pickup_weekday_num,
    d.pickup_weekday_name,

    CAST(NULL AS STRING) AS payment_type_name,
    CAST(NULL AS STRING) AS pu_borough_name,
    CAST(NULL AS STRING) AS trip_distance_categ,
    CAST(NULL AS INT64) AS trip_passenger_count,

    CAST(NULL AS INT64) AS pickup_hour,

    CAST(NULL AS INT64) AS pu_location_id,
    CAST(NULL AS STRING) AS pu_zone_name,
    CAST(NULL AS STRING) AS pu_zone_borough_name,
    CAST(NULL AS GEOGRAPHY) AS geometry,

    d.trip_count,
    CAST(NULL AS FLOAT64) AS trip_duration_minutes_sum,
    CAST(NULL AS INT64) AS total_passengers,
    CAST(NULL AS FLOAT64) AS total_amount_sum,
    CAST(NULL AS FLOAT64) AS trip_distance_sum,
    CAST(NULL AS FLOAT64) AS tip_amount_sum,

    CAST(NULL AS INT64) AS congestion_surcharge_only_trip_count,
    CAST(NULL AS INT64) AS airport_fee_only_trip_count,
    CAST(NULL AS INT64) AS both_special_charges_trip_count,
    CAST(NULL AS INT64) AS no_special_charges_trip_count,
    CAST(NULL AS FLOAT64) AS congestion_surcharge_sum,
    CAST(NULL AS FLOAT64) AS airport_fee_sum

FROM daily_series d

UNION ALL

-- ============================================================
-- HOURLY
-- ============================================================

SELECT
    'hourly' AS row_type,

    h.pickup_date AS date_anchor,
    h.pickup_date,

    CAST(NULL AS INT64) AS pickup_weekday_num,
    CAST(NULL AS STRING) AS pickup_weekday_name,
    CAST(NULL AS STRING) AS payment_type_name,
    CAST(NULL AS STRING) AS pu_borough_name,
    CAST(NULL AS STRING) AS trip_distance_categ,
    CAST(NULL AS INT64) AS trip_passenger_count,

    h.pickup_hour,

    CAST(NULL AS INT64) AS pu_location_id,
    CAST(NULL AS STRING) AS pu_zone_name,
    CAST(NULL AS STRING) AS pu_zone_borough_name,
    CAST(NULL AS GEOGRAPHY) AS geometry,

    h.trip_count,
    CAST(NULL AS FLOAT64) AS trip_duration_minutes_sum,
    CAST(NULL AS INT64) AS total_passengers,
    CAST(NULL AS FLOAT64) AS total_amount_sum,
    CAST(NULL AS FLOAT64) AS trip_distance_sum,
    CAST(NULL AS FLOAT64) AS tip_amount_sum,

    CAST(NULL AS INT64) AS congestion_surcharge_only_trip_count,
    CAST(NULL AS INT64) AS airport_fee_only_trip_count,
    CAST(NULL AS INT64) AS both_special_charges_trip_count,
    CAST(NULL AS INT64) AS no_special_charges_trip_count,
    CAST(NULL AS FLOAT64) AS congestion_surcharge_sum,
    CAST(NULL AS FLOAT64) AS airport_fee_sum

FROM hourly h

UNION ALL

-- ============================================================
-- MAP
-- ============================================================

SELECT
    'map' AS row_type,

    p.start_date AS date_anchor,
    CAST(NULL AS DATE) AS pickup_date,
    CAST(NULL AS INT64) AS pickup_weekday_num,
    CAST(NULL AS STRING) AS pickup_weekday_name,
    CAST(NULL AS STRING) AS payment_type_name,
    CAST(NULL AS STRING) AS pu_borough_name,
    CAST(NULL AS STRING) AS trip_distance_categ,
    CAST(NULL AS INT64) AS trip_passenger_count,

    CAST(NULL AS INT64) AS pickup_hour,

    m.pu_location_id,
    m.pu_zone_name,
    m.pu_zone_borough_name,
    m.geometry,

    m.trip_count,
    CAST(NULL AS FLOAT64) AS trip_duration_minutes_sum,
    CAST(NULL AS INT64) AS total_passengers,
    CAST(NULL AS FLOAT64) AS total_amount_sum,
    CAST(NULL AS FLOAT64) AS trip_distance_sum,
    CAST(NULL AS FLOAT64) AS tip_amount_sum,

    CAST(NULL AS INT64) AS congestion_surcharge_only_trip_count,
    CAST(NULL AS INT64) AS airport_fee_only_trip_count,
    CAST(NULL AS INT64) AS both_special_charges_trip_count,
    CAST(NULL AS INT64) AS no_special_charges_trip_count,
    CAST(NULL AS FLOAT64) AS congestion_surcharge_sum,
    CAST(NULL AS FLOAT64) AS airport_fee_sum

FROM map m

CROSS JOIN params p
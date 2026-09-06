{% macro cleanup_retention() %}

    {% set retention_months = var('retention_months', 24) %}
    {% if execute %}

        -- 1. determinar ventana de retencion desde dwh
        {% set bounds_query %}
            select
                max(source_file_month) as latest_month,
                date_sub(
                    max(source_file_month),
                    interval {{ retention_months - 1 }} month
                ) as cutoff_month
            from {{ source('bronze', 'yellow_trips') }}

        {% endset %}

        {% set bounds = run_query(bounds_query) %}

        {% set latest_month = bounds.columns[0].values()[0] %}
        {% set cutoff_month = bounds.columns[1].values()[0] %}


        {% if latest_month is none %}

            {{ log(
                "Retention cleanup skipped: no batches found in bronze.",
                info=True
            ) }}

        {% else %}

            {{ log(
                "Latest batch: " ~ latest_month|string,
                info=True
            ) }}

            {{ log(
                "Retention cutoff: " ~ cutoff_month|string,
                info=True
            ) }}


            --- 2. agregados de gold
            {% do run_query(
                "delete from " ~ ref('agg_taxi_trips_daily')
                ~ " where pickup_date < date('" ~ cutoff_month ~ "')"
            ) %}

            {% do run_query(
                "delete from " ~ ref('agg_taxi_trips_hourly')
                ~ " where pickup_date < date('" ~ cutoff_month ~ "')"
            ) %}

            {% do run_query(
                "delete from " ~ ref('agg_pickup_zone_daily')
                ~ " where pickup_date < date('" ~ cutoff_month ~ "')"
            ) %}


            -- 3. fact gold
            {% do run_query(
                "delete from " ~ ref('fct_taxi_trips')
                ~ " where pickup_date < date('" ~ cutoff_month ~ "')"
            ) %}

            -- 4. silver
            {% do run_query(
                "delete from " ~ ref('yellow_trips')
                ~ " where source_file_month < date('" ~ cutoff_month ~ "')"
            ) %}

            -- 5. bronze
            {% do run_query(
                "delete from " ~ source('bronze', 'yellow_trips')
                ~ " where source_file_month < date('" ~ cutoff_month ~ "')"
            ) %}


            {{ log(
                "Retention cleanup completed successfully.",
                info=True
            ) }}

        {% endif %}

    {% endif %}

{% endmacro %}
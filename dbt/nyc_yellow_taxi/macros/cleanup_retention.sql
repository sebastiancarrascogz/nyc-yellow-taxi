{% macro cleanup_retention() %}

    {% set retention = var('retention') %}
    {% set detail_months = retention['detail_months'] %}
    {% set aggregate_months = retention['aggregate_months'] %}

    {% set retention_mode = var('retention_mode', 'standard') %}
    {% set batch_month = var('batch_month', none) %}

    {% if execute %}

        {% set bounds_query %}

            select
                max(source_file_month) as latest_month,

                date_sub(
                    max(source_file_month),
                    interval {{ detail_months - 1 }} month
                ) as standard_detail_cutoff,

                date_sub(
                    max(source_file_month),
                    interval {{ aggregate_months - 1 }} month
                ) as aggregate_cutoff

            from {{ source('bronze', 'yellow_trips') }}

        {% endset %}

        {% set bounds = run_query(bounds_query) %}

        {% set latest_month = bounds.columns[0].values()[0] %}
        {% set standard_detail_cutoff = bounds.columns[1].values()[0] %}
        {% set aggregate_cutoff = bounds.columns[2].values()[0] %}


        {% if latest_month is none %}

            {{ log(
                "Retention cleanup skipped: no batches found.",
                info=True
            ) }}

        {% else %}

            {# Determinar cutoff del detalle #}

            {% if retention_mode == 'backfill' %}

                {% if batch_month is none %}
                    {{ exceptions.raise_compiler_error(
                        "batch_month is required in backfill mode"
                    ) }}
                {% endif %}

                {% set detail_cutoff = batch_month %}

            {% else %}

                {% set detail_cutoff = standard_detail_cutoff %}

            {% endif %}


            {{ log(
                "Retention mode: " ~ retention_mode,
                info=True
            ) }}

            {{ log(
                "Detail cutoff: " ~ detail_cutoff|string,
                info=True
            ) }}

            {{ log(
                "Aggregate cutoff: " ~ aggregate_cutoff|string,
                info=True
            ) }}


            {# GOLD AGGREGATES: 24 meses #}

            {% do run_query(
                "delete from " ~ ref('agg_taxi_trips_daily')
                ~ " where pickup_date < date('" ~ aggregate_cutoff ~ "')"
            ) %}

            {% do run_query(
                "delete from " ~ ref('agg_taxi_trips_hourly')
                ~ " where pickup_date < date('" ~ aggregate_cutoff ~ "')"
            ) %}

            {% do run_query(
                "delete from " ~ ref('agg_pickup_zone_daily')
                ~ " where pickup_date < date('" ~ aggregate_cutoff ~ "')"
            ) %}


            {# DETAIL #}

            {% do run_query(
                "delete from " ~ ref('fct_taxi_trips')
                ~ " where pickup_date < date('" ~ detail_cutoff ~ "')"
            ) %}

            {% do run_query(
                "delete from " ~ ref('yellow_trips')
                ~ " where source_file_month < date('" ~ detail_cutoff ~ "')"
            ) %}

            {% do run_query(
                "delete from " ~ source('bronze', 'yellow_trips')
                ~ " where source_file_month < date('" ~ detail_cutoff ~ "')"
            ) %}


            {{ log(
                "Retention cleanup completed successfully.",
                info=True
            ) }}

        {% endif %}

    {% endif %}

{% endmacro %}
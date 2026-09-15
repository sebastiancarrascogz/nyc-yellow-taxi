# Configuración del dashboard

Esta carpeta contiene la capa de serving SQL utilizada por el dashboard en Looker Studio.

## Archivos

- `looker_serving_query.sql`: consulta parametrizada de BigQuery usada como fuente principal del dashboard.

## Parámetros

La consulta recibe los filtros del dashboard mediante:

- `DS_START_DATE`
- `DS_END_DATE`
- `p_payment_type`
- `p_pu_borough`
- `p_do_borough`
- `p_distance`
- `p_passengers`

## Tipos de fila

La serving layer expone distintos granos mediante `row_type`:

- `daily`: KPIs y análisis categóricos.
- `daily_series`: series diarias y promedios por día de semana, incluyendo días sin viajes.
- `hourly`: promedios horarios, incluyendo horas sin viajes.
- `map`: métricas por zona de origen unidas a las geometrías de zonas TLC.

El archivo SQL debe mantenerse sincronizado con la Custom Query configurada en el Dashboard.
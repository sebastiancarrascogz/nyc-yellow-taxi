# NYC Yellow Taxi

Pipeline de datos de **NYC Yellow Taxi Trip Records** construido sobre una arquitectura **Medallion**, con ingesta mediante `dlt`, transformaciones con `dbt`, almacenamiento en **BigQuery**, orquestación con **Prefect** y visualización final en **Looker Studio**.

El pipeline está diseñado para procesar los archivos Parquet mensuales publicados por NYC TLC de forma incremental, idempotente y reproducible.

## Arquitectura

```text
NYC TLC Parquet
      │
      ▼
     dlt
      │
      ▼
┌─────────────┐
│   Bronze    │
│  BigQuery   │
└──────┬──────┘
       │
       ▼
      dbt
       │
       ▼
┌─────────────┐
│   Silver    │
│  BigQuery   │
└──────┬──────┘
       │
       ▼
      dbt
       │
       ▼
┌─────────────┐
│    Gold     │
│  BigQuery   │
└──────┬──────┘
       │
       ▼
 Looker Studio
```

La ejecución del pipeline es coordinada mediante Prefect:

```text
monthly_taxi_pipeline
        │
        ▼
   ingest_month
        │
        ▼
run_dbt_transformations
        │
        ▼
   run_dbt_tests
```


## Estructura principal

```text
nyc-yellow-taxi/
├── nyc_yellow_taxi/
│   ├── ingestion/
│   │   ├── load_yellow_trips.py
│   │   └── load_taxi_zones_shp.py
│   │
│   └── orchestration/
│       ├── monthly_pipeline.py
│       └── deploy.py
│
├── dbt/
│   └── nyc_yellow_taxi/
│       ├── models/
│       ├── seeds/
│       └── tests/
│
├── .dlt/
├── pyproject.toml
├── poetry.lock
└── README.md
```

## Instalación

Instalar las dependencias del proyecto:

```bash
poetry install
```

El proyecto requiere credenciales válidas para acceder a Google Cloud / BigQuery y a los recursos utilizados por `dlt`.

Los secretos y credenciales no deben almacenarse en el repositorio.

## Ingesta

La ingesta descarga los archivos Parquet mensuales publicados por NYC TLC y los carga en Bronze mediante `dlt`.

La unidad operacional del pipeline es un **mes**, representado mediante:

```text
source_file_month
```

### Ejecución manual

```bash
poetry run ingest-yellow-trips
```

Esta ejecución utiliza las fechas configuradas en los settings del proyecto.

La reingesta de un mismo `source_file_month` reemplaza el batch existente mediante una estrategia `delete-insert`.

## dbt

El proyecto dbt se encuentra en:

```text
dbt/nyc_yellow_taxi/
```

Los siguientes comandos deben ejecutarse desde ese directorio:

```bash
cd dbt/nyc_yellow_taxi
```

### Ejecutar un batch incremental

Ejemplo para junio de 2024:

```bash
poetry run dbt run \
  --select yellow_trips+ \
  --vars 'batch_month: 2024-06-01'
```

La selección:

```text
yellow_trips+
```

ejecuta el modelo Silver `yellow_trips` y todos sus modelos downstream en Gold.

### Ejecutar tests

```bash
poetry run dbt test --select yellow_trips+
```

### Compilar un modelo

Permite inspeccionar el SQL generado por dbt sin ejecutar el modelo:

```bash
poetry run dbt compile \
  --select fct_taxi_trips \
  --vars 'batch_month: 2024-06-01'
```

### Full refresh

Ejemplo:

```bash
poetry run dbt run \
  --select yellow_trips \
  --full-refresh
```

`--full-refresh` reconstruye completamente el modelo y omite la lógica incremental de `is_incremental()`.

## Incrementalidad

### Bronze

Cada archivo Parquet mensual representa un batch identificado mediante:

```text
source_file_month
```

La reingesta de un mismo mes reemplaza el batch correspondiente en Bronze.

Los registros conservan además:

```text
ingested_at
```

como metadata de la ejecución de ingesta.

### Silver

Silver utiliza:

```text
materialization: incremental
strategy: merge
unique_key: trip_id
```

`trip_id` identifica cada viaje mediante un fingerprint generado a partir de atributos estables del registro.

Cuando existe más de una versión del mismo `trip_id`, la precedencia utilizada es:

```text
1. source_file_month
2. ingested_at
```

Esto evita que un backfill o una reingesta histórica sobrescriba una versión proveniente de un batch más reciente.

La tabla Silver se encuentra físicamente optimizada mediante:

```text
PARTITION BY source_file_month
CLUSTER BY trip_id
```

La deduplicación también prioriza registros sin montos negativos cuando existen representaciones equivalentes dentro del mismo batch.

### Gold Fact

`fct_taxi_trips` utiliza:

```text
materialization: incremental
strategy: merge
unique_key: trip_id
```

La tabla está optimizada para consultas analíticas mediante:

```text
PARTITION BY pickup_date
CLUSTER BY trip_id
```

### Gold Aggregates

Las tablas agregadas utilizan:

```text
incremental_strategy: insert_overwrite
```

Los modelos detectan las `pickup_date` afectadas por el batch actual y reconstruyen completamente esas particiones a partir del estado vigente de `fct_taxi_trips`.

Esto permite mantener idempotencia y soportar registros tardíos sin acumular métricas incorrectamente al reprocesar un mismo batch.

Los principales agregados son:

```text
agg_taxi_trips_daily
agg_taxi_trips_hourly
agg_pickup_zone_daily
```

## Tests

El proyecto utiliza tests genéricos de dbt para validar propiedades como:

```text
not_null
unique
accepted_values
```

Silver incluye además unit tests para validar la lógica de precedencia incremental entre distintas versiones de un mismo `trip_id`.

Entre los escenarios probados se encuentran:

```text
batch antiguo + versión más reciente existente
→ el batch antiguo no reemplaza el registro

batch más nuevo + versión antigua existente
→ el batch más nuevo sí reemplaza el registro
```

Gold incluye además un test de reconciliación entre la fact y el agregado diario:

```text
COUNT(fct_taxi_trips)
=
SUM(agg_taxi_trips_daily.trip_count)
```

Esto permite detectar pérdida o duplicación de viajes durante la agregación.

## Prefect

La orquestación principal se encuentra en:

```text
nyc_yellow_taxi/orchestration/monthly_pipeline.py
```

El flow ejecuta:

```text
ingest_month
      │
      ▼
run_dbt_transformations
      │
      ▼
run_dbt_tests
```

La ingesta posee retries para tolerar fallos transitorios de red.

Las transformaciones y tests dbt fallan inmediatamente cuando el proceso devuelve un código de salida distinto de cero.

### Ejecución local del flow

```bash
poetry run python -m nyc_yellow_taxi.orchestration.monthly_pipeline
```

Si no se entrega explícitamente un `batch_month`, el flow calcula automáticamente el mes objetivo considerando un desfase de dos meses respecto del mes actual.

Por ejemplo:

```text
Fecha actual:
2026-08

Batch objetivo:
2026-06-01
```

Un `batch_month` explícito puede utilizarse para reprocesos o backfills.

## Prefect Server local

Levantar el servidor:

```bash
poetry run prefect server start
```

La UI queda disponible en:

```text
http://127.0.0.1:4200
```

Configurar el cliente local para utilizar ese servidor:

```bash
poetry run prefect config set \
  PREFECT_API_URL="http://127.0.0.1:4200/api"
```

## Work Pool y Worker local

Crear un Process Work Pool:

```bash
poetry run prefect work-pool create \
  local-process-pool \
  --type process
```

Levantar un worker conectado al pool:

```bash
poetry run prefect worker start \
  --pool local-process-pool
```

El worker queda escuchando ejecuciones asignadas al work pool y las ejecuta como procesos locales.

## Deployment local

La configuración del deployment se encuentra separada de la lógica del flow en:

```text
nyc_yellow_taxi/orchestration/deploy.py
```

Crear o actualizar el deployment:

```bash
poetry run python -m nyc_yellow_taxi.orchestration.deploy
```

El deployment puede ejecutarse posteriormente desde la UI de Prefect.

## Supuestos y limitaciones

- Para la operación normal del pipeline, cada archivo Parquet mensual de NYC TLC se trata como inmutable una vez ingerido.
- La reingesta de un mismo mes reemplaza el batch correspondiente en Bronze.
- Silver utiliza prioridad por `source_file_month` e `ingested_at` para evitar que batches antiguos sobrescriban versiones más recientes del mismo `trip_id`.
- Si un archivo histórico fuera republicado eliminando registros, esas eliminaciones no se propagan actualmente de forma automática desde Bronze hacia Silver.
- La propagación de eliminaciones históricas se considera fuera del alcance de la versión actual y queda registrada como mejora futura.
- La ejecución automática actualmente calcula el batch objetivo utilizando un desfase esperado de dos meses respecto del mes actual.

## Estado actual

El pipeline puede ejecutarse end-to-end mediante Prefect:

```text
NYC TLC
   ↓
  dlt
   ↓
Bronze
   ↓
  dbt
   ↓
Silver
   ↓
 Gold
   ↓
dbt tests
   ↓
Pipeline Completed
```

La siguiente etapa del proyecto contempla desplegar la orquestación utilizando **Prefect Cloud como control plane y un worker persistente en infraestructura propia**.
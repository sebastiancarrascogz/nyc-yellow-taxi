# nyc-yellow-taxi
Datos de NYC Yellow Taxi Trip en una arquitectura Lakehouse (Medallion) con visualización en Looker.

## Supuestos y limitaciones

- Para la operación normal del pipeline, cada archivo Parquet mensual de NYC TLC se trata como inmutable una vez ingerido.
- La reingesta de un mismo mes reemplaza el batch correspondiente en Bronze.
- Silver utiliza lógica incremental con prioridad por `source_file_month` e `ingested_at` para evitar que batches antiguos sobrescriban versiones más recientes del mismo `trip_id`.
- Actualmente, si un archivo histórico fuera republicado eliminando registros, esas eliminaciones no se propagan automáticamente desde Bronze hacia Silver. Este escenario queda fuera del alcance de la versión actual del pipeline y se considera una mejora futura.
# Jobs de Spark

Transformaciones del lakehouse. Spark corre en un contenedor efímero (ADR 0004): el host no
necesita Java. El perfil `core` del compose tiene que estar levantado.

## Uso

```powershell
scripts\spark-submit.ps1 pipelines/spark_jobs/bronze_load.py --dataset produccion_pozo
scripts\spark-submit.ps1 pipelines/spark_jobs/bronze_load.py --dataset produccion_pozo --resource-id <uuid>
scripts\spark-submit.ps1 pipelines/spark_jobs/silver_load.py --contract produccion_pozo
uv run python scripts/check_lake.py --namespace silver   # verificación desde el host, sin Spark
```

Hay un `scripts/spark-submit.sh` equivalente para Git Bash. La primera corrida baja ~700 MB
de jars de Maven (varios minutos); quedan en el volumen `ivy-cache`.

## bronze_load

Copia los CSV crudos de `s3://landing` a tablas Iceberg `lake.bronze.*`, una partición por
recurso (`_resource_id`). Qué hace, en orden:

1. Lee del manifiesto de ingesta (Postgres, por JDBC) la última corrida `ok` de cada recurso.
2. Resuelve la tabla destino de cada recurso con `bronze_tables.yaml` (regex sobre el nombre).
3. Lee de cada tabla bronze qué recursos ya están cargados y con qué `_source_sha256`.
4. Carga solo los recursos nuevos o cuyo hash cambió. Correrlo dos veces no hace nada.
5. Por cada recurso: lee el CSV con `header=true` y todas las columnas string, agrega las
   columnas de linaje (`_resource_id`, `_source_key`, `_source_sha256`, `_ingest_date`,
   `_loaded_at`, `data_origin`) y reemplaza la partición del recurso.

## silver_load

Aplica un contrato de datos (`pipelines/contracts/*.yaml`, ADR 0005) sobre una tabla bronze
y escribe `lake.silver.*` tipada y particionada. Qué hace, por recurso pendiente:

1. Compara `_resource_id -> _source_sha256` entre bronze y silver: procesa solo lo nuevo o
   cambiado, igual que bronze.
2. Marca cada fila con `reject_reason` según los `min`/`max`/`allowed_values` del contrato.
   Las que violan algo van a `lake.silver.<tabla>_rejects` con sus strings originales.
3. Castea las filas que quedan al tipo del contrato, conserva el linaje y agrega
   `_silver_loaded_at`.
4. Deduplica por `primary_key` quedándose con la fila de `dedupe_by` más alto.
5. Corre los checks duros (nulos donde el contrato no los permite, columnas ausentes, clave
   duplicada, más de 1 % de rechazos): si alguno falla, no escribe y el job devuelve 1.
6. Reemplaza las particiones afectadas y registra la corrida en `lake.silver.dq_runs`.

`check_lake.py --namespace silver` muestra tablas, filas por partición, las últimas corridas
de `dq_runs` y la cuarentena agrupada por motivo.

## Decisiones

- **Bronze no tipa.** Todo entra como string y se conserva tal cual (incluidas filas basura).
  El casteo a Float64 y las reglas de calidad son de silver: si bronze tipa, un CSV mal
  formado se pierde antes de que alguien pueda auditarlo.
- **Idempotencia por hash, no por fecha.** El manifiesto ya distingue contenido nuevo de
  contenido repetido; bronze compara el `sha256` cargado contra el del manifiesto.
- **Una partición por recurso.** `overwritePartitions()` reemplaza el año que se recarga sin
  tocar el resto, y `write.spark.accept-any-schema` + `merge-schema` tolera que un año traiga
  columnas que otro no tiene (2006 y 2024 no comparten esquema exacto).
- **Una tabla por tipo de recurso** (`bronze_tables.yaml`). El dataset `produccion_pozo`
  mezcla los anuales de DDJJ con "No Convencional" (un subconjunto de los anuales: en la
  misma tabla duplicaría filas), "Capítulo IV - Pozos" (catálogo de pozos) y el padrón de
  primera producción (tres columnas). Un recurso que no matchea ningún patrón se saltea con
  un WARNING: es preferible no cargarlo a cargarlo en la tabla equivocada.
- **En silver el YAML manda.** El job no tiene reglas propias: tipos, unicidad y rangos
  salen del contrato, y los rechazos se guardan en vez de descartarse (ADR 0005).
- **PyYAML en el runner**: `scripts/spark-submit.ps1`/`.sh` instalan las dependencias de
  `requirements-runner.txt` con `pip install --user` antes de cada corrida; quedan en el
  volumen `ivy-cache` y no se reinstalan (ADR 0004).
- **El manifiesto se lee por JDBC**, no con SQLAlchemy: la imagen del runner solo trae PySpark
  y la stdlib, y agregar dependencias Python al contenedor no vale la pena por una consulta.
- **BOM.** Los CSV del portal son UTF-8 con BOM y Spark no lo saca: el nombre de la primera
  columna se limpia a mano (`clean_column_name`).
- **Las funciones puras viven en `bronze_rules.py` y `silver_rules.py`** para poder testearlas
  sin JVM: las expresiones de casteo y de rechazo son strings de SQL, así que se comparan de
  a una en los tests. Los de `tests/spark_jobs/` no levantan Spark; la integración se valida
  corriendo el job.

# ADR 0009 — Gold se modela con dbt sobre la SparkSession del runner

**Estado:** aceptada · 2026-09-05

## Contexto

Bronze y silver son jobs de PySpark: leen una tabla, aplican reglas y escriben otra (ADR 0004,
ADR 0005). Gold es distinto. Son una decena de modelos dimensionales que dependen unos de
otros, cada uno con su documentación y sus tests, y lo que hay que poder contestar es "de dónde
sale esta columna". Escribir eso como otro job de PySpark significaría reimplementar a mano el
grafo de dependencias, el orden de ejecución, la documentación y los tests: exactamente lo que
dbt hace y hace bien.

La pregunta abierta era con qué adaptador. El ADR 0002 dijo `dbt-duckdb` en local y
`dbt-athena` en AWS, con DuckDB leyendo Iceberg desde MinIO. Eso fue antes de que existieran el
catálogo Iceberg REST y el runner de Spark: con DuckDB, gold quedaría escrito en Parquet suelto
en `s3://lakehouse/gold/` y habría que registrarlo aparte en el catálogo, mientras bronze y
silver son tablas Iceberg registradas. Dos formas de escribir la misma capa.

## Decisión

**dbt-spark con `method: session`, corriendo dentro del mismo runner efímero que bronze y
silver.** No suma ningún servicio: es un `spark-submit` más.

`method: session` significa que el adaptador no abre ninguna conexión (ni Thrift, ni ODBC, ni
Databricks): cada consulta hace `SparkSession.builder.getOrCreate()` y usa la sesión que
encuentre en el proceso. `pipelines/dbt/run_dbt.py` aprovecha eso: crea la sesión con
`build_spark()` —la misma función que usan bronze y silver— y recién después invoca a dbt. Como
la sesión ya trae el catálogo `lake` configurado y `spark.sql.defaultCatalog=lake`, gold se
escribe como tablas Iceberg en `lake.gold`, con la misma configuración que las capas de abajo,
sin que dbt sepa nada del asunto.

Dos detalles que costaron una vuelta y quedan escritos:

- Se invoca con `spark-submit run_dbt.py` y no con `python3 -m dbt.cli.main`. La imagen
  `apache/spark` no trae PySpark instalado como paquete: vive en `/opt/spark/python` y es
  `spark-submit` el que lo pone en el `PYTHONPATH` junto con el gateway de py4j. Con `python3`
  pelado, `import pyspark` falla.
- En `requirements-runner.txt` va `dbt-spark` sin el extra `[session]`. Lo único que agrega ese
  extra es `pyspark`, que la imagen ya trae: instalarlo de PyPI serían 400 MB duplicados que
  además taparían el PySpark de la imagen.

El perfil (`pipelines/dbt/profiles.yml`) está versionado con el repo y no tiene secretos: el
destino local no los necesita —la sesión ya viene armada con las credenciales del entorno— y el
destino `aws` usaría el rol de la cuenta. El target `aws` queda esbozado y comentado.

## Consecuencias

- Gold es indistinguible de bronze y silver para el resto del stack: `scripts/check_lake.py
  --namespace gold` la lee sin cambios, y el DAG `gold_mensual` lanza el mismo runner que los
  demás (ADR 0006), con un comando distinto.
- `scripts/dbt.ps1` y `scripts/dbt.sh` son gemelos de `spark-submit.ps1`/`.sh`. Un solo patrón
  de ejecución para todo el repo.
- El repo se monta read-only en el runner, así que `target/` y `logs/` de dbt van al volumen
  persistente (`/home/spark/dbt`), no al lado del proyecto. `dbt docs generate` deja ahí el
  catálogo y el manifiesto.
- `threads: 1`. El driver de Spark pide 4 GB y la máquina tiene 16: dos modelos en paralelo no
  ganan tiempo, se pelean la RAM. Los modelos corren de a uno.
- **Cambia el ADR 0002 en la mitad local.** DuckDB deja de ser el motor de la capa de modelado
  y queda para lo que sigue siendo bueno: consultas exploratorias desde el host y tests que no
  quieren levantar una JVM. En AWS no cambia nada: el destino sigue siendo Athena, que lee las
  mismas tablas Iceberg desde el Glue Data Catalog.
- El destino `aws` quedó resuelto aparte, en el **ADR 0010**: `dbt-athena` dentro de un job de
  Glue. El SQL específico de un motor sigue viviendo en un solo lugar, que pasó a ser
  `pipelines/dbt/macros/dialecto.sql`.

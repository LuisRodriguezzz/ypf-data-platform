# ADR 0011 — Streaming con Kafka y Spark Structured Streaming

**Estado:** aceptada · 2026-09-06

## Contexto

El RTIC de YPF recibe telemetría de 13 equipos concurrentes por Starlink, con cortes de enlace que hacen que parte de las lecturas lleguen tarde. El módulo de tiempo real del proyecto tiene que reproducir ese escenario en local, con datos reales, y dejar dos salidas en el lakehouse: el crudo tal como llegó y una agregación por ventana de tiempo.

La fuente es el dataset **3W de Petrobras** (CC BY 4.0): telemetría real de pozos a 1 Hz, 27 sensores, con eventos anómalos etiquetados por expertos (`docs/fuentes/telemetria_3w.md`). No hay telemetría pública de pozos argentinos.

## Decisión

**Kafka (un broker, KRaft) y no Kinesis local ni Redpanda.** El perfil `streaming` del compose levanta un solo `apache/kafka:4.1.2` en modo KRaft: es broker y controller a la vez, sin Zookeeper, sin UI y sin registro de esquemas. Kinesis solo existe en local a través de emuladores de terceros (localstack, kinesalite) que no son el servicio real y no aportan nada que después se defienda en una entrevista; Kafka sí es lo que corre en producción en la mayoría de las plataformas de datos, y `spark-sql-kafka-0-10` es la fuente de streaming mejor soportada de Spark. Redpanda sería más liviano, pero la imagen oficial de Apache con dos listeners y un healthcheck cuesta 30 líneas de compose y es la referencia contra la que se documenta todo.

El topic `telemetria_pozo` tiene **13 particiones** —los 13 equipos concurrentes— y la clave del mensaje es el `idpozo`, así todas las lecturas de un pozo caen en la misma partición y llegan ordenadas. (Con 13 claves y 13 particiones el hash no reparte una por una: en la práctica quedan particiones con dos pozos y otras vacías. Da igual: lo que importa es el orden por pozo.)

**Structured Streaming y no Flink.** El lakehouse ya es Spark de punta a punta (ADR 0001 y 0004): el mismo runner, el mismo catálogo Iceberg, la misma configuración por variables de entorno y el mismo código que después corre en Glue. Flink tiene mejor semántica de tiempo (watermarks por partición, estado más fino), pero sumaría un segundo motor, un segundo runtime en el compose y un segundo camino a AWS (MSF) para resolver una agregación por ventana de un minuto. El micro-batch de Spark, con trigger de 20 segundos, es de sobra para 13 pozos a 1 Hz.

**Dos queries, no una.** `lake.bronze.telemetria_pozo` (append, todos los eventos crudos, particionada por día de `event_time`) y `lake.silver.telemetria_pozo_1min` (una fila por pozo y minuto, con `withWatermark`). Cada una con su checkpoint en `s3a://lakehouse/checkpoints/<query>/`, así un reinicio retoma el offset guardado y no duplica filas en bronze.

**Watermark de 2 minutos.** Es el orden de magnitud de un corte de enlace satelital: cubre la reconexión típica sin obligar a Spark a mantener estado de las últimas horas. Con ventanas de 1 minuto y modo `append`, una ventana se emite dos minutos de tiempo de evento después de cerrarse.

## Qué es real y qué es simulado

| Pieza | Origen |
|---|---|
| Valores de los 27 sensores, `class`, `state`, frecuencia de 1 Hz | **Real** — pozos de Petrobras, dataset 3W, CC BY 4.0 |
| `idpozo`, empresa, yacimiento y cuenca a los que se asocia cada serie | **Simulado** — mapeo ficticio a pozos no convencionales de la Neuquina de `lake.silver.produccion_pozo` (`lake.bronze.pozo_map_3w`, `data_origin = 'simulated'`) |
| `event_time` | **Derivado** — el timestamp original rebaseado al arranque del replay; el original viaja en `event_time_3w` |
| Eventos que llegan tarde | **Simulado** — el productor retiene una fracción configurable (`--late-fraction`) |

Las dos tablas del streaming llevan `data_origin = 'simulated'` para que nadie confunda esta serie con telemetría de un pozo de YPF.

## Consecuencias

- **El `event_time` se rebasea.** Los archivos de 3W son de años distintos (2013 a 2019). Si se publicara el timestamp original, el watermark —que avanza con el máximo visto— tiraría como tardío todo lo que viniera de un archivo más viejo. El productor manda `event_time` contado desde el arranque del replay y conserva el original en `event_time_3w`, así la trazabilidad al Parquet de origen no se pierde.
- **`--speed` comprime el reloj de pared, no el tiempo de evento.** A `--speed 60`, un minuto de reloj son 60 minutos de tiempo de evento (1 Hz por pozo, como la fuente). Consecuencia práctica: la tolerancia real a los tardíos es `watermark + trigger × speed` —el watermark que Spark aplica en un micro-batch es el que calculó en el anterior—, o sea 2 min + 20 s × 60 ≈ 22 minutos de tiempo de evento. Por eso un corte de 30 a 120 segundos **no pierde ninguna fila** a 60x: en la corrida de validación de 10 minutos, 23.301 eventos retenidos (5 %) entraron todos en su ventana. Para que el watermark descarte hay que simular un corte largo (`--late-min 1800 --late-max 3600`) o correr a `--speed 1`. Es el resultado que se espera: un watermark de 2 minutos está para cubrir la reconexión típica, y el corte largo es el caso que hay que ver caer.
- **Lo tardío se descarta de la agregación pero queda en bronze.** Silver mide el descarte con `numRowsDroppedByWatermark`; bronze no filtra nada, así que siempre se puede reprocesar una ventana desde el crudo.
- **El catálogo SQLite es el cuello de botella de las dos queries.** SQLite (ADR 0003) acepta un solo escritor, y cuando los dos commits se pisan el catálogo REST devuelve 500, que Iceberg traduce a `CommitStateUnknownException` y mata la query (no se puede reintentar: no se sabe si el commit entró). Cuatro medidas: (0) las queries no arrancan juntas —silver espera medio trigger—, porque el batch caro es el primero, el que recupera atraso; (1) los triggers son distintos —bronze cada `--trigger` segundos, silver cada `--trigger + 1`—, porque Spark alinea los micro-batches a múltiplos absolutos del intervalo y con el mismo número los dos commits caen siempre en el mismo instante; (2) `maxOffsetsPerTrigger` acota el batch, así el primer micro-batch después de un reinicio no se traga todo el atraso de golpe con un commit largo; (3) `CATALOG_URI` pasó a `jdbc:sqlite:/data/iceberg_catalog.db?busy_timeout=30000` —sin el prefijo `file:`, porque sqlite-jdbc solo interpreta los pragmas de la query string cuando la URL trae el path pelado—. Cuando aun así se pisan, la conexión que perdió queda con la transacción abierta y **el catálogo deja de aceptar escrituras hasta que se reinicia el contenedor** (`podman stop`/`start ypf-lakehouse_iceberg-rest_1`); es lo primero que hay que mirar si un job empieza a fallar con 500 sin motivo. En AWS el catálogo es Glue y el problema no existe.
- **El productor es Python con `confluent-kafka`** (rueda compilada, con soporte para Windows en el host y para Linux/Python 3.10 en el runner), con `enable.idempotence`: sin eso un reintento de librdkafka sobre un mensaje que el broker ya escribió deja el evento duplicado en el topic, y "eventos publicados == filas en bronze" dejaría de cerrar. Corre en los dos lados: en el host contra `localhost:29092` y en el runner contra `kafka:9092`. El consumidor no necesita cliente Python: Spark habla con Kafka por el jar `spark-sql-kafka-0-10_2.13:4.0.4`.
- **El broker no tiene volumen.** El log del topic vive en la capa escribible del contenedor: la demo se apaga al terminar y `kafka-init` vuelve a crear el topic en el próximo `up`. Un volumen nuevo lo crea Podman como root y el proceso de Kafka (uid 1000) no podría escribirlo.
- **Airflow no orquesta el streaming.** Un consumidor de Structured Streaming es un proceso largo, no una tarea con principio y fin; meterlo en un DAG sería usar el orquestador como supervisor de servicios. Se lanza con `scripts/streaming-demo.*`.

## Alternativas descartadas

- **Kinesis local (emuladores):** no es el servicio real, no hay una historia de portabilidad honesta y agrega una dependencia de terceros.
- **Flink:** mejor motor de streaming, segundo runtime para el único job de streaming del proyecto.
- **Spark en `foreachBatch` escribiendo las dos tablas en una sola query:** evitaría la contención del catálogo, pero mezcla en un mismo bloque el crudo y la agregación con estado, y obliga a manejar a mano la deduplicación por `batchId`.
- **Escribir bronze desde el productor:** perdería la razón de ser del ejercicio (el consumidor y su checkpoint son lo que hace que un reinicio no duplique).

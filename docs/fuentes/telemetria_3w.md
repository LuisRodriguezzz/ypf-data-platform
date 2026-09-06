# Fuente: telemetría de pozos 3W (Petrobras)

Dataset **3W** de Petrobras: telemetría real de pozos de petróleo con eventos anómalos
etiquetados por especialistas, publicada en [github.com/petrobras/3W](https://github.com/petrobras/3W).
Es la fuente del módulo de tiempo real (ADR 0011): no existe telemetría pública de pozos
argentinos, y esta es la única candidata que verificamos con licencia sin ambigüedad, datos
reales de oil & gas y un paper revisado por pares (`research/gap-1.md`).

## Licencia y atribución

Dos licencias distintas, confirmadas textualmente en el README del repositorio: el **código**
del toolkit está bajo Apache 2.0 y **los datos** —los Parquet de los subdirectorios de
`dataset/`— bajo **Creative Commons Attribution 4.0 International (CC BY 4.0)**:

> "all 3W Dataset's data files (Parquet files saved in subdirectories of the dataset
> directory) are licensed under the Creative Commons Attribution 4.0 International License"

CC BY 4.0 permite uso comercial y en portfolio con atribución. La cita que pide el proyecto
(CITATION.md):

- Vargas, R. E. V. et al. *"A realistic and public dataset with rare undesirable real events
  in oil wells"*, Journal of Petroleum Science and Engineering, 181 (2019).
  DOI [10.1016/j.petrol.2019.106223](https://doi.org/10.1016/j.petrol.2019.106223)
- *"3W Dataset 2.0.0: a realistic and public dataset with rare undesirable real events in oil
  wells"*, Scientific Data 13, 949 (2026).
  DOI [10.1038/s41597-026-07225-z](https://doi.org/10.1038/s41597-026-07225-z)

## Qué es real y qué no

La telemetría es real: son pozos de Petrobras. **El pozo argentino al que se la asocia es
ficticio.** `pipelines/streaming/pozo_map.py` reparte los pozos de 3W entre los 13 primeros
`idpozo` no convencionales de la cuenca Neuquina de YPF con producción en el último año
declarado de `lake.silver.produccion_pozo`, y lo deja escrito en `lake.bronze.pozo_map_3w`
con `data_origin = 'simulated'`. Las dos tablas del streaming llevan la misma marca.

Dentro del dataset, los archivos `WELL-*` son **instancias reales**; `SIMULATED_*` y
`DRAWN_*` son sintéticas o dibujadas a mano. **Solo se bajan los `WELL-*`.**

## Estructura del dataset

Una carpeta por clase de evento (`dataset/<clase>/`), un Parquet por instancia, nombrado
`WELL-000NN_<timestamp de inicio>.parquet`. Conteo completo verificado archivo por archivo
contra la API de GitHub (`research/gap-1.md`):

| Clase | Nombre en `dataset.ini` | Instancias reales | Total archivos | Tamaño |
| --- | --- | ---: | ---: | ---: |
| 0 | NORMAL | 594 | 594 | 162 MB |
| 1 | ABRUPT_INCREASE_OF_BSW | 4 | 128 | 230 MB |
| **2** | **SPURIOUS_CLOSURE_OF_DHSV** | **22** | 38 | 18,5 MB |
| 3 | SEVERE_SLUGGING | 32 | 106 | 187,5 MB |
| 4 | FLOW_INSTABILITY | 343 | 343 | 63 MB |
| 5 | RAPID_PRODUCTIVITY_LOSS | 11 | 450 | 420,5 MB |
| 6 | QUICK_RESTRICTION_IN_PCK | 6 | 221 | 168 MB |
| **7** | **SCALING_IN_PCK** | **36** | 46 | 137 MB |
| 8 | HYDRATE_IN_PRODUCTION_LINE | 14 | 95 | 152,7 MB |
| 9 | HYDRATE_IN_SERVICE_LINE | 57 | 207 | 332,8 MB |

## Subconjunto que usa el proyecto

`pipelines/streaming/fetch_3w.py --classes 0,2,7` con los cupos por defecto (10 / 22 / 10).
Medido sobre `landing` el 2026-09-06:

| Clase | Archivos | MB en landing | Pozos distintos |
| --- | ---: | ---: | ---: |
| 0 — normal | 10 | 6,1 | 9 |
| 2 — cierre espurio de DHSV | 22 | 4,6 | 7 |
| 7 — scaling en el PCK | 10 | 23,0 | 6 |
| **Total** | **42** | **33,7** | **18** (WELL-00001 a 13, 19, 21 a 24) |

Las tres clases se eligieron por criterio, no por tamaño: 0 es la línea de base normal, 2 es
un evento de válvula (cierre espurio del DHSV) que se ve en las presiones en segundos, y 7
es una degradación lenta (incrustación en el choke de producción) que un modelo tiene que
detectar por tendencia. Los archivos no se toman por orden alfabético sino repartidos entre
pozos distintos: la clase 0 tiene 594 archivos de solo 9 pozos y los primeros 10 por nombre
saldrían todos de WELL-00001.

En landing quedan como `3w/class=<clase>/<archivo>.parquet`, con una fila por archivo en
`ingestion_manifest` (dataset `telemetria_3w`, `resource_id = <clase>/<archivo>`). La
idempotencia usa el **sha del blob de git**, que la API de GitHub devuelve en el listado y se
guarda en `last_modified_source`: si el archivo no cambió, no se vuelve a bajar.

## Columnas

30 columnas: 27 sensores (todos `double`), la etiqueta `class`, el estado operativo `state`
(ambos `int16`) y `timestamp` (`Datetime[ns]`, sin zona).

| Grupo | Columnas | Qué son |
| --- | --- | --- |
| Presiones | `P-PDG`, `P-TPT`, `P-MON-CKP`, `P-JUS-CKP`, `P-MON-CKGL`, `P-JUS-CKGL`, `P-JUS-BS`, `P-MON-SDV-P`, `P-ANULAR`, `PT-P` | En Pa. `P-PDG` es el permanent downhole gauge (fondo de pozo); `P-TPT` el temperature/pressure transducer; `-MON-` y `-JUS-` son aguas arriba y aguas abajo de cada choke o válvula |
| Temperaturas | `T-TPT`, `T-PDG`, `T-MON-CKP`, `T-JUS-CKP` | En °C |
| Aperturas | `ABER-CKP`, `ABER-CKGL` | Apertura porcentual del choke de producción y del de gas lift |
| Estados de válvula | `ESTADO-DHSV`, `ESTADO-M1`, `ESTADO-M2`, `ESTADO-PXO`, `ESTADO-SDV-GL`, `ESTADO-SDV-P`, `ESTADO-W1`, `ESTADO-W2`, `ESTADO-XO` | 0/1 (cerrada/abierta) codificados como `double` |
| Caudales | `QGL`, `QBS` | Gas lift y bombeo |
| Etiquetas | `class`, `state` | Ver abajo |

`class` toma el número de la clase del evento (0 = normal, N = evento N) y además **100+N**,
que marca el **transitorio previo** al evento N. No es ruido: es la etiqueta que hace posible
detectar el evento *antes* de que ocurra, y hay que modelarla, no descartarla.

En el módulo de streaming los nombres viajan en minúsculas y con guión bajo (`P-MON-CKP` →
`p_mon_ckp`): un guión medio obliga a backticks en cada consulta SQL.

## Frecuencia y rarezas medidas

- **1 Hz exacto**, verificado sobre `WELL-00002_20131104004101.parquet`: 12.721 filas, 3 h
  32 min, un único delta entre muestras de 1 segundo (semana 0). El README oficial no lo
  declara en ningún lado; es medición propia.
- **Muchos sensores nulos en los archivos viejos.** En ese mismo archivo de 2013, 23 de los
  27 sensores están enteramente en nulo y `P-PDG` vale 0 constante. El pipeline no puede
  asumir el esquema completo: el productor omite las claves sin valor y el consumidor las
  completa con `null` desde el esquema explícito.
- **`class` y `state` también vienen nulos** en el arranque de las instancias (3.600 filas
  del archivo citado, la primera hora).
- **Valores `NaN`** aparecen en algunos sensores. Se descartan al construir el evento:
  `json.dumps(nan)` escribe `NaN`, que no es JSON válido y rompería el `from_json` de Spark.
- **Duración muy despareja entre instancias**: van de una a ocho horas de registro. A 60x,
  las más cortas (la mayoría de las de clase 2) se agotan a los tres minutos de reloj y el
  caudal del replay se desploma; por eso `replay_3w` tiene `--loop`, que vuelve a empezar el
  archivo cuando se termina para que los 13 pozos sigan midiendo a 1 Hz.
- Los archivos son de **años distintos** (2013 a 2019) y de pozos distintos, así que no
  comparten eje de tiempo. Por eso el replay rebasea el tiempo de evento y conserva el
  original en `event_time_3w` (ADR 0011).

## Medición del replay (2026-09-06)

Corrida de validación de 10 minutos a `--speed 60` con 13 pozos simultáneos y 5 % de eventos
retenidos entre 30 y 120 segundos de tiempo de evento:

| Métrica | Valor |
| --- | ---: |
| Eventos publicados en Kafka | 468.001 |
| Caudal sostenido | 780 ev/s (13 pozos × 60x × 1 Hz) |
| Retenidos como tardíos y reenviados | 23.301 (5,0 %) |
| Filas en `lake.bronze.telemetria_pozo` | 468.001 (= publicados, sin duplicados ni pérdidas) |
| Ventanas en `lake.silver.telemetria_pozo_1min` | 7.774 |
| Descartados por el watermark | 0 |
| Tiempo de evento cubierto | 10 horas (600 s de reloj × 60) |

Cero descartes es el resultado esperado: la tolerancia real a un tardío es
`watermark + trigger × speed` = 2 min + 20 s × 60 ≈ 22 minutos de tiempo de evento, así que
un corte de hasta 2 minutos entra siempre. Para ver caer filas hay que simular un corte largo
(`--late-min 1800 --late-max 3600`), y esas filas igual quedan enteras en bronze.

Segunda corrida, de 5 minutos, con cortes largos (30 a 60 minutos de tiempo de evento) y el
consumidor **matado a mitad de camino** (`podman kill` sobre el runner) y vuelto a levantar:

| Métrica | Valor |
| --- | ---: |
| Eventos publicados (5 min) | 234.001 |
| Filas acumuladas en bronze (las dos corridas) | 702.002 |
| Eventos publicados en total (468.001 + 234.001) | 702.002 |
| Descartados por el watermark (corte de 30-60 min) | 4.588 |

Bronze quedó exactamente igual al total publicado después de tres reinicios del consumidor
—uno por `podman kill` y dos por fallas del catálogo (ADR 0011)—: el checkpoint retoma el
offset donde estaba y no reprocesa lo ya escrito. Los 4.588 eventos que el watermark descartó
de la agregación **están enteros en bronze**, que es justamente para lo que sirve tener el
crudo.

## Cómo se usa

```powershell
uv run python -m pipelines.streaming.fetch_3w --classes 0,2,7   # landing + manifiesto
uv run python -m pipelines.streaming.pozo_map                   # lake.bronze.pozo_map_3w
scripts\streaming-up.ps1                                        # core + broker de Kafka
scripts\streaming-demo.ps1 -Segundos 600 -Velocidad 60          # productor + consumidor
```

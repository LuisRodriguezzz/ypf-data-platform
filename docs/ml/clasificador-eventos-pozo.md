# Modelo: clasificador de eventos de pozo sobre telemetría a 1 Hz

Qué contesta: **mirando 180 segundos de telemetría de un pozo, ¿el pozo está normal, está en el
transitorio que precede a un evento no deseado, o el evento ya está ocurriendo?**

Es la otra mitad del proyecto respecto de
[`modelo-completacion-produccion.md`](modelo-completacion-produccion.md): aquel modela una
decisión de ingeniería que se toma una vez por pozo; este mira lo que el pozo está haciendo
ahora mismo. Y tiene una pregunta que el otro no puede tener: **cuánto antes del evento avisa.**

Decisiones de arquitectura en [ADR 0013](../adr/0013-clasificador-eventos-3w.md). Código en
`pipelines/ml/telemetria_features.py`, `entrenar_eventos.py` y `detectar_eventos.py`.

## Los datos

Origen: el dataset **3W** de Petrobras, telemetría real de pozos de petróleo con eventos
etiquetados por especialistas ([`docs/fuentes/telemetria_3w.md`](../fuentes/telemetria_3w.md),
CC BY 4.0). El entrenamiento lee los Parquet directo de `landing` (`3w/class=*/`); la inferencia
lee `lake.bronze.telemetria_pozo`, que es lo mismo pasado por Kafka.

Una **instancia** es un archivo: un pozo registrado de corrido durante horas o días, a 1 Hz, con
la etiqueta `class` en cada segundo.

| Clase de 3W | Qué es | Instancias | Con evento | Duración mediana |
| --- | --- | ---: | ---: | ---: |
| 0 | Normal | 30 | — | 5 h 55 |
| 2 | Cierre espurio del DHSV | 22 | 11 | 2 h 56 |
| 7 | Scaling en el choke de producción | 36 | 5 | 46 h 6 |
| **Total** | | **88** | **16** | |

Son 18 pozos distintos (`WELL-00001` a `13`, `19`, `21` a `24`) y 8,5 millones de lecturas.

Las dos clases de evento se eligieron por criterio y no por tamaño: la 2 es un evento de válvula
que se ve en las presiones en segundos, y la 7 es una degradación lenta que hay que detectar por
tendencia. El modelo **no distingue entre las dos**: distingue en qué etapa está el pozo, que es
lo que decide si hay que avisar.

**Lo que no llega a evento importa.** De las 36 instancias de clase 7, 31 terminan en el
transitorio: el especialista marcó que el pozo se estaba degradando pero el registro corta antes
de la falla. Se usan igual, porque el transitorio es una etiqueta tan real como el evento.

### Las etiquetas

`class` vale `0` en el pozo normal, `N` durante el evento N y **`100+N` durante el transitorio
previo**. Esas tres se colapsan a las tres etiquetas del modelo —`normal`, `transitorio`,
`evento`— y la etiqueta de una ventana es **la más severa que toca**. Como las instancias de 3W
son monótonas (normal, después transitorio, después evento), eso coincide con el estado en el
que la ventana termina, pero no depende de ese supuesto.

Las filas con `class` en nulo —la primera hora de muchas instancias— no se pueden etiquetar y
sus ventanas se descartan.

### Las ventanas

**180 segundos con paso de 15 s**, que es lo que sugiere el toolkit de 3W para la clase 2. Cada
punto de la serie entra en 12 ventanas superpuestas.

Con un tope de **1.500 ventanas por instancia**: las de clase 7 duran hasta 213 horas y con
paso de 15 s una sola aportaría 51.000 ventanas. Se ensancha el paso de esa instancia en vez de
recortarla, para conservar sus tres etapas.

| Etiqueta | Ventanas | % |
| --- | ---: | ---: |
| `normal` | 39.916 | 44,4 % |
| `transitorio` | 48.251 | 53,7 % |
| `evento` | **1.712** | **1,9 %** |
| **Total** | **89.879** | |

De dónde sale cada una:

| Clase de 3W | `normal` | `transitorio` | `evento` |
| --- | ---: | ---: | ---: |
| 0 | 25.229 | — | — |
| 2 | 5.689 | 5.724 | 1.246 |
| 7 | 8.998 | 42.527 | 466 |

El desbalance no es un accidente del muestreo: el transitorio de una instancia de clase 7 dura
días y el evento, minutos. Se corrige con `class_weight="balanced"` y se mide con **F1 macro**,
donde las tres clases pesan igual.

**85 instancias de las 88 aportan ventanas.** Las tres de `WELL-00008` no traen ninguno de los
cinco sensores del modelo y quedan afuera enteras: es el filtro `con_datos`, que descarta la
ventana en la que ningún sensor midió nada.

### Las features

Cinco sensores × seis estadísticos = **30 features**. Ni una que dependa de saber qué archivo
es o qué pozo lo registró.

Los sensores se eligieron por cobertura medida sobre las 88 instancias (instancias con al menos
un valor) y por qué mueve cada evento:

| Sensor | Qué es | Cobertura |
| --- | --- | ---: |
| `t_tpt` | Temperatura en el transducer de fondo | 84/88 |
| `p_tpt` | Presión en el transducer de fondo | 80/88 |
| `p_anular` | Presión del anular | 76/88 |
| `p_pdg` | Presión en el permanent downhole gauge | 75/88 |
| `p_mon_ckp` | Presión aguas arriba del choke de producción | 66/88 |

`p_jus_ckp` —la presión aguas abajo del choke, el par natural de `p_mon_ckp` para ver el
scaling— está poblada en **1 de las 88 instancias**. No se puede usar, y esa es la limitación
física del dataset, no una decisión.

Los seis estadísticos por sensor: **media, desvío, mínimo, máximo, pendiente** (regresión lineal
simple contra el tiempo, en unidades por segundo) y **delta** (último menos primer valor de la
ventana). Los cuatro primeros describen el nivel; los dos últimos, cómo se movió, que es lo
único que separa una degradación lenta de un valor alto y estable.

Un sensor que la instancia no trae deja sus seis features en `NaN`. No se imputan: el
`HistGradientBoostingClassifier` aprende hacia qué rama mandar el faltante, y un cero imputado
sería un valor que el instrumento nunca midió.

## Cómo se evaluó

**`GroupKFold` de 5 folds sobre `instancia_id`.** Ninguna instancia aparece a la vez en train y
en test. El motivo es directo: dos ventanas consecutivas comparten 165 de sus 180 segundos, así
que con un split aleatorio casi toda ventana de test tiene su gemela en train.

Se reporta además el **split por pozo** (`well_3w`), donde ni siquiera el pozo se repite entre
train y test. Es el que describe el uso real: un pozo al que se le acaba de instalar el
sistema.

| Modelo | Split | F1 macro out-of-fold |
| --- | --- | ---: |
| Clase mayoritaria (siempre `transitorio`) | por instancia | 0,233 |
| Umbral sobre un sensor (`p_tpt_desvio`) | por instancia | 0,481 |
| **HistGradientBoosting** | **por instancia** | **0,712** |
| HistGradientBoosting | **por pozo** | **0,414** |

Los dos baselines están para acotar el mérito. El de umbral es un árbol de profundidad 1 sobre
el desvío de la presión del TPT dentro de la ventana: literalmente *"si la presión se movió más
que X, avisá"*, que es la regla que pondría un operador sin modelo. Le acierta razonablemente a
`normal` y a `transitorio` (F1 0,70 y 0,75) y **nunca predice `evento`**: eso es exactamente lo
que hay que superar.

Los **30 puntos de diferencia entre el split por instancia y el split por pozo** son el hallazgo
incómodo del trabajo y están discutidos en las limitaciones.

## Resultados

### Por clase (HistGradientBoosting, split por instancia, 89.879 ventanas)

| Etiqueta | Precisión | Recall | F1 | Soporte |
| --- | ---: | ---: | ---: | ---: |
| `normal` | 0,770 | 0,839 | 0,803 | 39.916 |
| `transitorio` | 0,858 | 0,793 | 0,824 | 48.251 |
| `evento` | 0,506 | 0,512 | **0,509** | 1.712 |

### Matriz de confusión

| | pred. `normal` | pred. `transitorio` | pred. `evento` |
| --- | ---: | ---: | ---: |
| real `normal` | **33.500** | 5.817 | 599 |
| real `transitorio` | 9.718 | **38.277** | 256 |
| real `evento` | 295 | 541 | **876** |

Los dos errores que importan se leen acá:

- **9.718 ventanas de transitorio clasificadas como normales** (20 % del transitorio). Casi
  todas son de clase 7: un pozo que empieza a incrustar el choke se ve normal durante horas, y
  180 segundos de ventana no alcanzan para ver la tendencia de un día.
- **295 ventanas de evento clasificadas como normales** (17 % del evento). Son las que un
  sistema de alerta se pierde del todo. Otras 541 se clasifican como transitorio, que en la
  práctica también dispara la alarma: el 83 % de las ventanas de evento producen alguna alerta.

El costo está en la primera fila: 599 ventanas normales clasificadas como `evento` (1,5 % de
las normales) y 5.817 como `transitorio` (14,6 %). Un pozo produce 5.760 ventanas por día con
paso de 15 s, así que ese 1,5 % son unas **86 alarmas de evento espurias por día y por pozo** si
se alertara ventana por ventana. Un consumidor real tiene que agrupar ventanas consecutivas
antes de despertar a nadie; `gold.alerta_evento_pozo` deja las ventanas crudas y no hace esa
agrupación.

### Tiempo de anticipación

Cuánto antes del **primer segundo de evento** el modelo levanta la mano. La alarma es la primera
ventana out-of-fold que no salió `normal`, medida contra el **fin** de esa ventana: el primer
instante en que un consumidor en línea la habría tenido.

De las **16 instancias con evento**:

| Dónde cayó la primera alarma | Instancias |
| --- | ---: |
| Dentro del transitorio (a tiempo) | **8** |
| Antes de que empezara el transitorio | 6 |
| Después de que empezara el evento | 0 |
| Nunca alarmó | 2 |

Anticipación de las 8 alarmas a tiempo:

| Clase de 3W | Instancias | Media | Mediana |
| --- | ---: | ---: | ---: |
| 2 — cierre espurio del DHSV | 7 | **1 h 40 min** (5.997 s) | 1 h 37 min (5.791 s) |
| 7 — scaling en el choke | 1 | 33 h 02 min (118.907 s) | — |

Las 6 alarmas anteriores al transitorio **no se cuentan como anticipación**, y esa es una
decisión deliberada. Una alarma que aparece antes de que el especialista marque el inicio del
transitorio da un número enorme y no es mérito: es un falso positivo que quedó del lado
correcto. Cinco de esas seis caen a menos de dos horas del inicio del transitorio y una a 65
segundos, así que probablemente sean detecciones tempranas legítimas; contarlas igual sería
elegir la interpretación que conviene.

Las 2 instancias que nunca alarmaron son de clase 7 y son el peor resultado del modelo: el pozo
se degradó durante horas sin que ninguna ventana saliera de `normal`.

### Qué pesa, según la importancia por permutación

Cuánto cae el F1 macro al desordenar cada feature (modelo final, 5 repeticiones):

| # | Feature | Caída de F1 macro | Desvío |
| ---: | --- | ---: | ---: |
| 1 | `p_mon_ckp_media` | 0,0572 | 0,0005 |
| 2 | `t_tpt_media` | 0,0548 | 0,0006 |
| 3 | `p_anular_media` | 0,0282 | 0,0004 |
| 4 | `p_pdg_media` | 0,0280 | 0,0001 |
| 5 | `p_mon_ckp_maximo` | 0,0211 | 0,0004 |
| 6 | `t_tpt_minimo` | 0,0205 | 0,0003 |
| 7 | `t_tpt_maximo` | 0,0163 | 0,0002 |
| 8 | `p_tpt_maximo` | 0,0135 | 0,0005 |
| 9 | `p_tpt_minimo` | 0,0127 | 0,0006 |
| 10 | `p_tpt_media` | 0,0120 | 0,0009 |

**Lectura, y es la que importa:** las diez primeras features son de **nivel** (media, mínimo,
máximo). Ninguna pendiente ni ningún delta entra en el top 10; la primera feature de forma es
`p_mon_ckp_desvio`, en el puesto 11 y con un tercio del peso de la primera. El modelo está
reconociendo sobre todo *en qué régimen de presión y temperatura está el pozo*, y solo
secundariamente *cómo se está moviendo*. Es coherente con la caída al pasar al split por pozo, y
es la limitación número uno.

## La inferencia

`pipelines/ml/detectar_eventos.py` arma las mismas ventanas —con las mismas funciones— sobre las
últimas 24 horas de `event_time` de `lake.bronze.telemetria_pozo` y escribe las que no salieron
`normal` en `lake.gold.alerta_evento_pozo`.

Corrida de validación del 2026-09-06, sobre las 702.002 lecturas que el replay dejó en bronze
(ADR 0011):

| Métrica | Valor |
| --- | ---: |
| Lecturas leídas | 702.002 |
| Pozos | 13 |
| Ventanas clasificadas | 28.657 |
| ...`normal` | 22.738 |
| ...`transitorio` | 4.806 |
| ...`evento` | 1.113 |
| **Filas escritas en `gold.alerta_evento_pozo`** | **5.919** |
| Pozos con alguna alerta | 7 |

Las alertas se concentran en 7 de los 13 pozos, que es lo esperable: el replay reparte entre
los pozos archivos de las tres clases y solo algunos están reproduciendo instancias con evento.

**Esto es batch, no streaming.** La alerta sale con horas de retraso. La inferencia en línea
sería un `foreachBatch` en el consumidor de Kafka; el ADR 0013 explica por qué no está y qué
habría que resolver para que esté.

## Limitaciones

Las cinco que hay que leer antes de usar cualquier número de arriba.

**1. El modelo reconoce regímenes de pozo, no solo dinámica.** Es lo que dicen las dos cosas a
la vez: que las features más importantes sean las medias de los sensores, y que el F1 macro baje
de 0,712 a **0,414** cuando el split es por pozo en vez de por instancia. Con 18 pozos, el
modelo puede aprender el nivel típico de cada uno y usarlo como atajo. En un pozo nuevo, sin
historia, hay que esperar el 0,414 y no el 0,712. Normalizar cada sensor contra su propia línea
de base por pozo es la mejora obvia y no está hecha.

**2. Son 16 eventos.** Todas las métricas de la clase `evento` —F1 0,509— y toda la tabla de
anticipación salen de 16 instancias, 11 de una clase y 5 de la otra. Las 1.712 ventanas de
evento parecen muchas pero son 16 sucesos vistos con lupa. Un intervalo de confianza honesto
sobre la anticipación media de la clase 2 con 7 observaciones es muy ancho, y el único número de
clase 7 sale de **una sola instancia**.

**3. Los pozos argentinos son ficticios.** La telemetría es real y es de Petrobras. El `idpozo`
que aparece en `gold.alerta_evento_pozo` sale del mapeo ficticio de
`pipelines/streaming/pozo_map.py` (`data_origin = 'simulated'` en la tabla de mapeo). Nada de
esto describe a un pozo de YPF.

**4. La ventana de 180 segundos es corta para la clase 7.** El scaling del choke se desarrolla
en horas o días y el modelo lo mira de a tres minutos. Se ve en los dos errores más grandes: las
9.718 ventanas de transitorio clasificadas como normales y las 2 instancias que nunca alarmaron.
Features multi-escala —la misma ventana a 180 s, 30 min y 6 h— son la segunda mejora obvia y
tampoco están hechas.

**5. La anticipación se mide contra la etiqueta de un especialista, no contra la falla.** El
"inicio del evento" es el segundo en que alguien de Petrobras decidió que el evento había
empezado, mirando la serie completa y hacia atrás. Es la mejor referencia disponible y es
subjetiva. Además, en las instancias largas de clase 7 el paso de la ventana se ensancha para
respetar el tope de 1.500 ventanas por instancia, así que la anticipación de esas instancias se
mide con una resolución de minutos y no de 15 segundos.

## Cómo se corre

```powershell
uv run python -m pipelines.streaming.fetch_3w --classes 0,2,7   # instancias a landing
uv run python -m pipelines.ml.entrenar_eventos                  # entrena y registra en MLflow
uv run python -m pipelines.ml.detectar_eventos --horas 24       # escribe gold.alerta_evento_pozo
uv run python scripts/check_lake.py --namespace gold --table alerta_evento_pozo
```

En Airflow es el DAG `eventos_pozo` (`@daily`), con las dos tareas en serie en el runner: si el
entrenamiento no supera al baseline termina con error y la detección no corre, así el lake nunca
se llena de alertas de un modelo que no se validó.

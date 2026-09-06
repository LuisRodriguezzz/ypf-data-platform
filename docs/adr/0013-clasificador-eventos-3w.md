# ADR 0013 — Los eventos de pozo se detectan con ventanas de 180 s, split por instancia y en batch

**Estado:** aceptada · 2026-09-06

## Contexto

El módulo de streaming (ADR 0011) deja en `lake.bronze.telemetria_pozo` telemetría real de
pozos a 1 Hz con la etiqueta de un especialista en cada segundo: el dataset 3W de Petrobras
(`docs/fuentes/telemetria_3w.md`). La etiqueta `class` distingue tres cosas en el tiempo: el
pozo normal (`0`), el **transitorio** que precede a un evento no deseado (`100+N`) y el
**evento** mismo (`N`). Esa tercera etiqueta es la que hace que el problema valga la pena: no
se trata de reconocer un evento que ya ocurrió, sino de avisar antes.

Los números que lo acotan, medidos sobre lo que hay en landing: **88 instancias** (un archivo
= un pozo registrado de corrido) de **18 pozos**, con 8,5 millones de lecturas. De esas 88, solo
**16 llegan a tener evento**: 11 de clase 2 (cierre espurio del DHSV) y 5 de clase 7 (scaling
en el choke de producción). Las otras 72 terminan en el transitorio o no salen nunca de normal.
Es un problema de pocos eventos, muchísimas muestras por evento y mucha correlación temporal.

## Decisión

### Por qué ventanas de features y no una red sobre la secuencia

La alternativa natural para telemetría es una red recurrente o una convolucional 1-D sobre la
serie cruda. No se hace, por tres razones que en este dataset pesan más que la elegancia:

- **Hay 16 eventos, no 16.000.** Una red sobre secuencias tiene que aprender de cero qué es
  una tendencia y qué es un pico, y necesita ejemplos independientes para hacerlo. Acá los
  ejemplos independientes son las instancias, y son 88. Las 89.879 ventanas que salen de ellas
  parecen muchas pero comparten 165 de cada 180 segundos con su vecina: el tamaño efectivo de
  la muestra es el número de instancias, no el de filas.
- **Media, desvío, mínimo, máximo, pendiente y diferencia entre extremos son exactamente lo que
  un especialista mira.** El toolkit de 3W sugiere ventanas de 180 s con paso de 15 s para la
  clase 2, y esos seis estadísticos por sensor son la descripción compacta de una ventana. Una
  red tendría que redescubrirlos.
- **Los sensores faltan de a bloques enteros.** En 3W los archivos viejos traen 23 de 27
  sensores enteramente en nulo, y la cobertura de los cinco que usa el modelo va del 95 % de
  las instancias (`t_tpt`) al 75 % (`p_mon_ckp`). El `HistGradientBoostingClassifier` come
  `NaN` de fábrica y aprende hacia qué rama mandarlo; en una red hay que imputar, y un cero
  imputado es un valor que el instrumento nunca midió.

Es además `scikit-learn` pelado, sin torch ni una segunda familia de dependencias que pinear
para el runner de Python 3.10 (ADR 0004). El mismo criterio del ADR 0012.

**Lo que se paga:** la ventana no ve más allá de 180 segundos, así que el modelo no puede usar
"esto viene subiendo desde hace seis horas" salvo por la pendiente de los últimos tres minutos.
Para el scaling del choke, que es una degradación de días, eso es una limitación real.

### Por qué el split es por instancia y no aleatorio

Dos ventanas consecutivas de la misma instancia comparten 165 de sus 180 segundos. Con un split
aleatorio, casi toda ventana de test tiene su gemela en train y el modelo aprueba el examen por
haber visto ese mismo minuto, no por reconocer el fenómeno. `GroupKFold` sobre `instancia_id`
deja cada archivo entero de un solo lado.

Se reporta además el split por **pozo** (`well_3w`), que es más duro todavía, y la diferencia
es la información más incómoda del trabajo: **F1 macro 0,712 con split por instancia contra
0,414 con split por pozo**. Esos 30 puntos son el nivel de presión y temperatura propio de cada
pozo, que el modelo usa como atajo —las features más importantes son las medias de los sensores,
no sus pendientes—. Los dos números se publican; el que describe el uso real —un pozo nuevo,
sin historia— es el 0,414.

El split por instancia se elige como métrica de decisión del alias `champion` porque es el que
compara modelos con la misma cantidad de datos de entrenamiento; el de pozo se reporta como
techo de realidad.

### Por qué la inferencia es batch y no un `foreachBatch` en el consumidor

La inferencia en línea de verdad sería un `foreachBatch` dentro de
`pipelines/streaming/consume_telemetria.py`: por cada micro-lote, mantener por pozo los últimos
180 s en estado (`flatMapGroupsWithState`), armar la ventana y clasificarla ahí mismo. La
alerta saldría en segundos en vez de en horas, y para un cierre de DHSV —que se ve en las
presiones en segundos— eso importa.

No se implementa ahora por dos razones concretas:

- **El modelo vive en el registry de MLflow.** Cargarlo dentro de un ejecutor de Spark obliga a
  resolver cómo llega el artefacto a cada worker y qué versión tiene cada uno; con el modelo
  cargado en el driver y difundido, cada cambio de versión exige reiniciar el streaming.
- **Mezcla dos ciclos de vida en un proceso.** El consumidor tiene su propio checkpoint y su
  propio watermark, y hoy se lo puede matar y levantar sin pensar (está medido en el ADR 0011).
  Meterle el modelo adentro significa redeployar el pipeline de datos cada vez que se reentrena
  el modelo, y viceversa.

`pipelines/ml/detectar_eventos.py` clasifica las últimas 24 horas de `event_time` y deja el
resultado en `lake.gold.alerta_evento_pozo` como una tabla Iceberg más, con
`data_origin = 'derived'`, igual que `prediccion_produccion_12m`. Cuando el caso de uso pida
segundos, lo que cambia es el motor: `construir_ventanas` es la misma función.

### Qué se escribe y qué no

Solo las ventanas que **no** salieron `normal`. La tabla se llama `alerta_evento_pozo` y es de
alertas: en la corrida de validación fueron 5.919 filas contra 28.657 ventanas clasificadas.
Quien quiera el estado completo de un pozo lo tiene en `lake.silver.telemetria_pozo_1min`.

La tabla se reemplaza entera en cada corrida, como la de predicciones: es una foto de las
últimas 24 horas con el modelo vigente, y volver a correr el DAG del mismo día da exactamente
el mismo resultado.

## Consecuencias

- **Ninguna instancia aporta más de 1.500 ventanas.** Las de clase 7 duran hasta 213 horas: con
  paso de 15 s una sola aportaría 51.000 ventanas y decidiría el dataset entero. Se ensancha el
  paso de esa instancia en vez de recortarla, así se conservan sus tres etapas —normal,
  transitorio y evento— a menor resolución temporal. El precio se paga en la anticipación: en
  esas instancias se mide con la resolución del paso ensanchado, que puede ser de minutos.
- **Las clases quedan muy desparejas y se corrige con `class_weight="balanced"`.** El evento es
  el 1,9 % de las ventanas (1.712 de 89.879) porque el transitorio de una instancia de clase 7
  dura días y el evento, minutos. La métrica de decisión es el **F1 macro**, donde las tres
  clases pesan igual: un modelo que acierte todo el transitorio y nada del evento no sirve para
  avisar y no debe promoverse.
- **El tiempo de anticipación se mide contra el primer segundo de evento y se reporta con
  dónde cayó la alarma.** Una alarma anterior al inicio del transitorio da una anticipación
  enorme y no es mérito: es un falso positivo que quedó del lado correcto. De las 16 instancias
  con evento, 8 alarmaron dentro del transitorio, 6 antes de que empezara y 2 no alarmaron
  nunca. Solo las 8 primeras entran en el promedio.
- **Sin grilla de hiperparámetros.** Con 88 instancias, el split por instancia deja folds de 17
  grupos; elegir hiperparámetros sobre esos mismos folds infla el número que después se publica.
  Es el razonamiento del ADR 0012 sobre la validación anidada, llevado un paso más lejos: acá
  ni siquiera se busca.
- **No hacen falta dependencias nuevas.** El clasificador usa las mismas seis librerías de ML
  pineadas en el ADR 0012 y ya instaladas en el runner; `shap` no se usa en este modelo porque
  SHAP multiclase sobre 90.000 ventanas es caro y la importancia por permutación mide lo que se
  quiere: cuánto cae el **F1 macro** —la métrica de decisión— al desordenar cada feature.
- **Las coordenadas de MLflow van en `registro_eventos.py` y no en `registro.py`.** Son dos
  modelos con ciclos de vida distintos —uno se reentrena por mes con el mart de gold, el otro
  por día con la telemetría— y un solo módulo con las constantes de los dos invita a que la
  inferencia de uno cargue el alias del otro. El server, el bucket de artefactos y el alias
  `champion` sí se comparten y se importan de `registro.py`.
- **Nada de esto viaja a AWS.** El equivalente sería el mismo job en Glue leyendo la misma
  tabla; el código no cambiaría porque el catálogo y el endpoint salen de `LakehouseConfig`,
  pero el ADR 0008 no lo cubre y queda fuera de alcance.

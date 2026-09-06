# ADR 0012 — El modelo es un gradient boosting tabular, se sigue en MLflow y se sirve en batch

**Estado:** aceptada · 2026-09-06

## Contexto

Gold deja un mart de features (`mart_pozo_completacion_produccion`, ADR 0009): un pozo
fracturado por fila, con el diseño de su completación de un lado y lo que produjo del otro. La
pregunta del proyecto —cuánto de la productividad temprana se explica por cómo se estimuló el
pozo— es una regresión tabular sobre esa tabla.

Los números que la acotan: **4.635 pozos fracturados, 3.825 no convencionales, y solo 351 con
los 12 meses de producción declarados**, repartidos en 43 yacimientos y 17 features. No es un
problema de datos grandes: es un problema de datos chicos y ruidosos.

## Decisión

### Por qué `HistGradientBoostingRegressor` y no deep learning

Con 351 filas y 17 columnas, una red neuronal tiene más parámetros que ejemplos y hay que
regularizarla hasta que se comporte como un modelo lineal. El gradient boosting sobre
histogramas es lo que gana en tabular a esta escala, y además trae de fábrica tres cosas que
en este dataset se necesitan y que en una red hay que construir a mano:

- **Nulos sin imputar.** Las intensidades derivadas (arena por metro, etapas por metro) son
  nulas en los pozos verticales, donde la rama horizontal vale 0. El HGB aprende hacia qué
  rama mandar el nulo; imputar con la mediana inventaría un pozo horizontal que no existe.
- **Categóricas nativas.** `formacion` y `tipo_terminacion` entran como enteros y el modelo
  parte por subconjunto, sin one-hot.
- **Escalas mezcladas.** Metros, toneladas, psi y HP conviven sin normalizar porque los
  árboles solo miran el orden.

Es además `scikit-learn` pelado: sin XGBoost, sin LightGBM y sin un framework de ML encima.
Una dependencia menos que pinear y que hacer entrar en el runner.

El baseline contra el que se compara son dos: predecir siempre la mediana del train y una
regresión lineal sobre las mismas features. La lineal está de adorno útil: falla feo (R² muy
negativo) y esa falla es la que muestra que la relación entre completación y producción no es
lineal.

### Por qué el split es por yacimiento y no aleatorio

Dos pozos del mismo yacimiento comparten la roca, y la roca explica buena parte de lo que
produce un pozo. Con un split aleatorio, casi todo pozo de test tiene un vecino en train: el
modelo aprueba el examen por saber dónde está el pozo, no por entender la completación.

La diferencia es medible y es grande: **R² 0,737 con `KFold` aleatorio contra 0,381 con
`GroupKFold` sobre `areayacimiento`**. Esos 36 puntos son la fuga, y publicar el 0,737 sería
publicar el error. Se reporta el número del split por grupo aunque sea la mitad de vistoso.

El uso real del modelo, además, es el del split por grupo: estimar qué va a producir un pozo
que se está por completar, muchas veces en un área donde todavía no hay historia.

### Por qué inferencia batch y no un servicio

El dato de entrada cambia una vez por mes, cuando corre `gold_mensual`: la producción se
declara mensualmente y las fracturas se recargan a diario pero se consolidan igual de lento.
Un endpoint HTTP contra un modelo que responde lo mismo durante 30 días es un servicio para
mantener, monitorear y desplegar a cambio de nada.

`pipelines/ml/predecir.py` predice para los 3.825 pozos no convencionales de una vez y deja el
resultado en `lake.gold.prediccion_produccion_12m` como una tabla Iceberg más, con
`data_origin = 'derived'`. Cualquier consumidor del lakehouse —DuckDB desde el host, Athena en
AWS, un dashboard— la lee como lee las otras. Si algún día hace falta un endpoint, el modelo
ya está en el registry y `mlflow models serve` lo levanta sin tocar este código.

Se predice para **todos** los pozos y no solo para los que se usaron de entrenamiento: el caso
de uso real es el pozo fracturado hace cuatro meses, que todavía no tiene el año cumplido. La
columna `prod_pet_12m_real` queda nula en esos y con valor en los que sí lo cumplieron, que es
lo que después permite medir la deriva.

### Dónde vive el modelo

Un MLflow propio en el perfil `mlflow` del compose, con el backend en la base `mlflow` de
Postgres —la misma instancia que ya guarda el manifiesto de ingesta y la metadata de Airflow—
y los artefactos en el bucket `mlflow` de MinIO. Es un contenedor más y ningún servicio nuevo
fuera del compose.

El backend en Postgres y no en archivos porque el **Model Registry no existe con un backend de
archivos**, y el registry es lo que hace que `predecir.py` no tenga que saber qué corrida
entrenó el modelo bueno: pide `models:/completacion_produccion_12m@champion` y le llega la
versión vigente. `entrenar.py` mueve ese alias solo si el modelo le gana al mejor baseline; si
no le gana, registra la versión sin alias y termina con error, así el DAG no sigue y el lake no
se llena de predicciones de un modelo que no se validó.

## Consecuencias

- **El host y el runner corren exactamente las mismas versiones.** El runner trae Python 3.10
  (ADR 0004) y un pipeline de scikit-learn guardado con 1.7.2 hay que cargarlo con 1.7.2, así
  que las seis dependencias de ML están pineadas en `pyproject.toml` a la última versión con
  rueda para 3.10 (`scikit-learn==1.7.2`, `pandas==2.3.3`, `numpy==2.2.6`, `shap==0.49.1`,
  `matplotlib==3.10.9`). Dejar que el host resolviera a algo más nuevo es la forma más directa
  de romper la inferencia sin enterarse hasta que corre el DAG.
- En el runner va `mlflow-skinny` y no `mlflow`: el cliente alcanza para loguear, registrar y
  cargar; el paquete completo suma el server (FastAPI, uvicorn, alembic) que ahí no corre. Hay
  que agregar `skops` a mano, porque es el formato con el que MLflow 3 guarda los modelos de
  scikit-learn y skinny no lo trae.
- **Los artefactos no pasan por el server.** Se probó `--serve-artifacts` para que MLflow
  hiciera de proxy contra MinIO: la subida funciona, pero la bajada devuelve una URL prefirmada
  contra el endpoint del server (`minio:9000`), que desde Windows no resuelve. El server usa
  `--default-artifact-root s3://mlflow/` y cada cliente habla con MinIO por su cuenta con el
  endpoint que ya sabe `LakehouseConfig` (`localhost:9000` en el host, `minio:9000` en el
  runner).
- `runner.py` reenvía al runner solo las variables de `FORWARDED_ENV` y `MLFLOW_TRACKING_URI`
  no está ahí. El DAG `ml_mensual` la pone delante del comando (`MLFLOW_TRACKING_URI=... python3
  -m ...`), que en el `bash -c` del runner es una asignación válida: una línea por tarea en vez
  de tocar la pieza que comparten los cinco DAGs.
- El modelo explica **el 38 % de la varianza fuera de muestra** y no más. Es un resultado
  honesto para 351 pozos con split por yacimiento, y sirve para ordenar diseños de completación
  entre sí; no sirve para prometerle a nadie los m3 de un pozo puntual. Las limitaciones están
  escritas en `docs/ml/modelo-completacion-produccion.md` y son parte del entregable.
- Nada de esto viaja a AWS todavía. El equivalente sería SageMaker o un MLflow sobre RDS/S3; el
  código de `pipelines/ml/` no cambiaría porque el catálogo y el endpoint ya salen de
  `LakehouseConfig`, pero el ADR 0008 no lo cubre y queda fuera de alcance.

# Modelo: producción de petróleo a 12 meses según el diseño de completación

Qué contesta: **dado cómo se va a estimular un pozo no convencional —cuántas etapas, cuánta
arena, cuánta agua, qué largo de rama— y dónde está, ¿cuántos m3 de petróleo va a acumular en
sus primeros 12 meses de producción?**

Es la pregunta que motiva todo el lakehouse. El hallazgo exploratorio que la disparó: en la
cuenca Neuquina no convencional, el petróleo acumulado a 12 meses crece **7 veces** entre los
pozos de menos de 20 etapas y los de más de 40. El modelo pone ese número a prueba con el
resto de las variables controladas y con una validación que no se hace trampa.

Decisiones de arquitectura en [ADR 0012](../adr/0012-ml-con-mlflow.md). Código en
`pipelines/ml/`.

## Los datos

Origen: `lake.gold.mart_pozo_completacion_produccion` (ADR 0009), un pozo fracturado por fila.

| Filtro | Pozos |
| --- | --- |
| Pozos fracturados en el mart | 4.635 |
| `tipo_de_recurso = 'NO CONVENCIONAL'` | 3.825 |
| ...y `meses_con_declaracion = 12` (**dataset de entrenamiento**) | **351** |

Los dos filtros tienen motivo:

- **No convencional.** En el convencional la fractura suele ser una intervención sobre un pozo
  que ya producía; la relación entre el diseño y la producción de los primeros 12 meses es
  otra cosa y mezclarlas sería modelar dos fenómenos con un solo modelo.
- **12 meses declarados.** Un pozo con 6 meses tiene el acumulado truncado. Entrenar con él le
  enseñaría al modelo que ese diseño produce poco, cuando lo que pasa es que le falta historia.

Reparto del dataset de entrenamiento: **43 yacimientos**, 333 pozos en Neuquina y 18 en
Austral; 269 en Vaca Muerta, 29 en Lajas, 18 en Magallanes, 18 en Mulichinco y el resto
repartido. 270 SHALE y 81 TIGHT. Fracturas entre 2011 y 2025.

### El target

`prod_pet_12m`, modelado como `log1p(prod_pet_12m)`. La distribución va de 0 a 155.502 m3 con
mediana 950: en escala original el error de los pozos grandes se come el de todos los demás y
el modelo se convierte en un predictor de los diez pozos más productivos.

**114 de los 351 pozos acumularon 0 m3 de petróleo en sus 12 meses.** La mayoría son pozos de
gas (Lajas, Mulichinco, Magallanes declaran gas y no petróleo), pero unos 57 declararon 12
meses de ceros en petróleo **y** en gas: son pozos cuyo padrón marca una primera producción
anterior a la producción efectiva. Se dejan porque el filtro documentado es el de los 12 meses
y sacarlos a mano sería elegir el subconjunto que le conviene al modelo; el efecto está medido
más abajo, en las limitaciones.

### Las features

17 columnas, ninguna que el pozo no conozca antes de empezar a producir.

**Diseño de la completación** (lo que se puede decidir): `cantidad_fracturas`,
`longitud_rama_horizontal_m`, `arena_bombeada_total_tn`, `agua_inyectada_m3`,
`co2_inyectado_m3`, `presion_maxima_psi`, `potencia_equipos_fractura_hp`, `duracion_dias`,
`tipo_terminacion`.

**Contexto** (lo que viene dado): `cuenca`, `formacion`, `sub_tipo_recurso`, `profundidad`,
`anio_fractura`.

**Intensidades derivadas**: `arena_por_metro`, `agua_por_etapa`, `etapas_por_metro`. Son las
tres medidas con las que la industria compara diseños entre pozos de largo distinto. Suben el
R² fuera de muestra de 0,304 a 0,337 con los mismos hiperparámetros, así que se quedan. En los
pozos verticales (rama = 0) quedan nulas a propósito y el modelo las trata como faltantes.

**Limpieza**: `presion_maxima_psi` se recorta en 20.000 y `potencia_equipos_fractura_hp` en
100.000, los topes físicos documentados en [`docs/fuentes/fractura.md`](../fuentes/fractura.md).
Hay 12 filas con presiones de hasta 209.640 psi y una con 232.159 HP: son errores de unidad, no
pozos monstruosos. Se recortan en vez de descartar el pozo, que tiene el resto de las columnas
sanas.

Las categóricas se codifican con `OrdinalEncoder` (`unknown_value=-1`) porque el modelo es de
árboles: parte por umbral sobre el código y no necesita la expansión one-hot.

## Cómo se evaluó

**`GroupKFold` de 5 folds sobre `areayacimiento`.** Ningún yacimiento aparece a la vez en train
y en test.

El motivo es concreto: dos pozos del mismo yacimiento comparten la roca, y la roca explica
buena parte de lo que produce un pozo. Con un split aleatorio casi todo pozo de test tiene un
vecino en train, y el modelo aprueba el examen por saber dónde está el pozo, no por entender la
completación. La diferencia medida sobre este dataset:

| Validación | R² (log) |
| --- | --- |
| `KFold` aleatorio | **0,737** |
| `GroupKFold` por yacimiento | **0,381** |

Esos 36 puntos son la fuga. Se reporta el 0,381.

Se miden dos R² distintos y hay que no confundirlos:

- **R² out-of-fold ("oof")**: se junta la predicción que cada pozo recibió cuando estaba en
  test y se calcula el R² sobre los 351. Usa la varianza del dataset entero y es el número
  comparable entre modelos.
- **R² por fold**: se calcula dentro de cada fold. Como los folds agrupan yacimientos y algunos
  yacimientos son homogéneos, la varianza local es chica y el R² sale bajo aunque el error en
  m3 sea el mismo. Se reporta igual porque muestra la dispersión entre regiones.

Hiperparámetros: una grilla de seis combinaciones, elegida por R² out-of-fold. La selección usa
los mismos folds que después se reportan, así que el número publicado es algo optimista; con
seis candidatos el sesgo es chico, y una validación anidada sobre 351 pozos dejaría folds de
test de 15 pozos. Ganó `max_iter=200, learning_rate=0.05, max_leaf_nodes=15,
min_samples_leaf=10`.

## Resultados

### Modelo contra baselines (predicción out-of-fold, 351 pozos)

| Modelo | R² log | R² m3 | MAE log | RMSE log | MAE m3 | RMSE m3 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Mediana del train | −0,212 | −0,335 | 4,017 | 4,835 | 13.451 | 24.538 |
| Regresión lineal | −11,590 | −2.574 | 4,801 | 15,586 | 152.782 | 1.077.636 |
| **HistGradientBoosting** | **0,381** | **0,191** | **2,617** | **3,457** | **9.417** | **19.098** |

El modelo le gana claramente a los dos baselines: explica el 38 % de la varianza del target en
log y baja el error absoluto medio de 13.451 a 9.417 m3.

La regresión lineal no es un error de implementación: usa exactamente las mismas features y
falla feo. Con el split por yacimiento, un fold la deja extrapolar fuera del rango que vio y la
predicción se dispara. Es la evidencia de que la relación entre completación y producción no es
lineal y de que el modelo de árboles está haciendo algo real.

### Por fold

| Fold | R² log | MAE log | RMSE log | R² m3 | MAE m3 | RMSE m3 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0,384 | 2,873 | 3,524 | 0,214 | 5.923 | 15.891 |
| 2 | 0,040 | 2,669 | 3,832 | −0,121 | 13.886 | 24.191 |
| 3 | 0,178 | 3,139 | 4,075 | 0,223 | 8.066 | 22.852 |
| 4 | 0,242 | 2,501 | 3,245 | 0,124 | 12.207 | 17.950 |
| 5 | 0,338 | 1,759 | 2,280 | 0,401 | 8.871 | 14.151 |
| **Promedio** | **0,236** | **2,588** | **3,391** | **0,168** | **9.791** | **19.007** |

El fold 2 es el peor (R² 0,040): agrupa yacimientos cuya producción el modelo no anticipa bien.
Que un fold quede casi en cero con 5 folds y 43 grupos es esperable y es información: el modelo
no es igual de bueno en todas las regiones.

### Qué pesa, según SHAP

|SHAP| medio por feature, en unidades de log(m3). Modelo final ajustado sobre los 351 pozos.

| # | Feature | \|SHAP\| medio |
| ---: | --- | ---: |
| 1 | `tipo_terminacion` | 1,267 |
| 2 | `anio_fractura` | 1,083 |
| 3 | `cantidad_fracturas` | 0,761 |
| 4 | `duracion_dias` | 0,700 |
| 5 | `profundidad` | 0,561 |
| 6 | `agua_por_etapa` | 0,450 |
| 7 | `potencia_equipos_fractura_hp` | 0,438 |
| 8 | `agua_inyectada_m3` | 0,344 |
| 9 | `presion_maxima_psi` | 0,318 |
| 10 | `longitud_rama_horizontal_m` | 0,283 |

`cuenca`, `co2_inyectado_m3` y `sub_tipo_recurso` quedan en 0,000: el modelo no las usa. Es
razonable —`formacion` ya contiene la información de cuenca y subtipo, y el CO2 es 0 en casi
todo el dataset—, pero conviene saberlo antes de leer la tabla como si midiera importancia
física.

La importancia por permutación (caída de R² al desordenar la columna) ordena parecido pero no
igual, y es la que mide impacto sobre el error y no sobre la predicción:

| Feature | Caída de R² | Desvío |
| --- | ---: | ---: |
| `anio_fractura` | 0,440 | 0,030 |
| `cantidad_fracturas` | 0,234 | 0,022 |
| `tipo_terminacion` | 0,184 | 0,018 |
| `agua_por_etapa` | 0,150 | 0,009 |
| `profundidad` | 0,109 | 0,007 |

**Lectura.** Las dos primeras posiciones no son variables de diseño: `anio_fractura` y
`tipo_terminacion` describen sobre todo *cuándo* se hizo la fractura y con qué tecnología, y
entre 2011 y 2025 el no convencional argentino cambió por completo. El modelo está capturando
en buena medida esa evolución. Recién en tercer lugar aparece la variable que el proyecto
quería medir —`cantidad_fracturas`—, y aparece de forma robusta en las dos métricas. El 7×
entre pozos de menos de 20 y más de 40 etapas es real (la correlación de las etapas con el
target en log es 0,586), pero **una parte de él es el año**: los pozos de muchas etapas son los
pozos modernos, y los pozos modernos son mejores por muchas razones a la vez.

`duracion_dias` en cuarto lugar es incómoda y hay que decirlo: es cuánto duró el tratamiento, y
un tratamiento largo es un tratamiento grande. No es una palanca de diseño, es un reflejo del
tamaño de la operación.

## Limitaciones

Las cinco que hay que leer antes de usar cualquier número de arriba.

**1. Correlación, no causalidad.** El modelo mide asociación en datos observacionales. Nadie
asignó etapas al azar: las operadoras ponen más etapas donde la roca es mejor y donde el
proyecto tiene más presupuesto. Que el modelo asocie más etapas con más producción **no
autoriza** a decir "si le pongo 20 etapas más a este pozo, va a producir un 40 % más". Para eso
haría falta un diseño experimental que la industria no tiene, o un análisis causal con
supuestos explícitos que este trabajo no hace.

**2. Sesgo de supervivencia y de selección.** Entrenar con los pozos que tienen 12 meses
declarados no es entrenar con una muestra aleatoria: son pozos que se completaron, se
conectaron y declararon todos los meses. Un pozo que se abandonó, que quedó esperando
infraestructura o cuya operadora declaró de forma irregular no está en el dataset. El modelo
estima la producción **condicionada a que el pozo llegue a producir 12 meses**, que es más
optimista que la producción incondicional.

**3. Tamaño de muestra.** 351 pozos y 43 yacimientos. Con 5 folds, cada fold de test tiene unos
70 pozos y 8 o 9 yacimientos: el intervalo de confianza del R² es ancho y la diferencia entre
los folds (0,040 a 0,384) es del mismo orden que la métrica. La cifra de 0,381 no está medida
con la precisión que sugieren sus tres decimales. Además, el número es chico porque en el lake
hay producción cargada de 2006 a 2026 pero la mayoría de los pozos no convencionales tiene
declaraciones incompletas en sus primeros 12 meses: al proyecto le faltan datos, no le sobran.

**4. Deriva temporal.** `anio_fractura` es la segunda feature más importante y va de 2011 a
2025. Un modelo que aprendió que "más nuevo es mejor" extrapola hacia adelante por definición
mal: para un pozo de 2027, el año es un valor que nunca vio y el HGB lo trata como el borde de
su último umbral. Es la razón por la que el DAG `ml_mensual` reentrena todos los meses, y la
razón por la que la tabla de predicciones guarda `prod_pet_12m_real` al lado de la predicción:
cuando esos pozos cumplan el año se va a poder medir cuánto se equivocó el modelo.

**5. Extrapolación fuera del rango entrenado.** El dataset de entrenamiento llega a 71 etapas.
Los pozos con mayor producción predicha tienen 90, 95 y 105 etapas: están fuera de rango y el
modelo de árboles, que no extrapola, les asigna el valor del borde. Sus predicciones (100.000 a
175.000 m3) hay que leerlas como "de los más productivos", no como una cifra.

**Y una que no es limitación del modelo sino del target**: el 32 % de los pozos de
entrenamiento acumuló 0 m3 de petróleo, casi todos porque producen gas. Un modelo de petróleo
entrenado sobre una mezcla de pozos de petróleo y de gas gasta capacidad en separar las dos
poblaciones. Lo natural sería modelar el gas por separado o predecir la energía equivalente;
está fuera del alcance de esta iteración y es la mejora más obvia que queda pendiente.

## Cómo se corre

```powershell
# Perfil de MLflow (una vez): UI en http://localhost:5000
podman-compose --profile core --profile mlflow up -d

# Entrenamiento: registra la corrida en MLflow y promueve el modelo si le gana al baseline
uv run python -m pipelines.ml.entrenar

# Inferencia batch: escribe lake.gold.prediccion_produccion_12m
uv run python -m pipelines.ml.predecir

uv run python scripts/check_lake.py --namespace gold --table prediccion_produccion_12m
```

En Airflow es el DAG `ml_mensual` (día 2 a las 7, después de `gold_mensual`), con las dos
tareas en el runner.

## La tabla de salida

`lake.gold.prediccion_produccion_12m`, `data_origin = 'derived'`, **3.825 filas** —todos los
pozos no convencionales, no solo los que se usaron para entrenar—, reemplazada entera en cada
corrida.

| Columna | Significado |
| --- | --- |
| `idpozo` | Pozo. Es el grano de la tabla |
| `prod_pet_12m_predicho` | m3 de petróleo estimados para los primeros 12 meses |
| `prod_pet_12m_real` | El acumulado real, **nulo** si el pozo todavía no declaró los 12 meses (3.474 de 3.825) |
| `modelo_version` | Versión del modelo en el registry que produjo la fila |
| `predicho_en` | Momento de la corrida |
| `data_origin` | `derived` |

Los 10 pozos con mayor producción predicha **entre los que todavía no cumplieron el año**
(3.474 de los 3.825):

| idpozo | sigla | yacimiento | etapas | meses declarados | predicción (m3) |
| ---: | --- | --- | ---: | ---: | ---: |
| 165830 | TPT.Nq.FP-1263(h) | FORTIN DE PIEDRA | 69 | 1 | 174.542 |
| 165831 | TPT.Nq.FP-1264(h) | FORTIN DE PIEDRA | 69 | 1 | 173.879 |
| 165829 | TPT.Nq.FP-1262(h) | FORTIN DE PIEDRA | 68 | 1 | 141.244 |
| 166779 | YPF.Nq.LACh-457(h) | LA AMARGA CHICA | 96 | 5 | 125.981 |
| 166782 | YPF.Nq.LACh-816(h) | LA AMARGA CHICA | 90 | 5 | 125.981 |
| 165832 | TPT.Nq.FP-1261(h) | FORTIN DE PIEDRA | 64 | 1 | 110.598 |
| 166964 | VIS.Nq.BPO-2364(h) | BAJADA DEL PALO OESTE | 105 | 1 | 103.679 |
| 166780 | YPF.Nq.LACh-458(h) | LA AMARGA CHICA | 95 | 5 | 101.685 |
| 162679 | SHE.Nq.CdL-36(h) | CRUZ DE LORENA | 41 | 8 | 99.152 |
| 166449 | YPF.Nq.LACh-417(h) | LA AMARGA CHICA | 93 | 9 | 97.466 |

Son todos pozos horizontales de Vaca Muerta con 40 a 105 etapas en los yacimientos shale más
activos de Neuquén: el modelo está señalando lo que la industria ya sabe, que es exactamente lo
que uno quiere ver antes de confiar en algo que también dice cosas que no sabe. Las dos
predicciones idénticas de La Amarga Chica (125.981 para dos pozos distintos) son dos pozos que
caen en la misma hoja del árbol; con 96 y 90 etapas los dos están fuera del rango entrenado
(máximo 71) y el modelo los satura en el mismo valor de borde.

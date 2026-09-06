"""DAG mensual del módulo de ML: reentrenar y volver a predecir sobre el gold del mes.

Dos tareas en serie en el mismo runner que el resto del stack (ADR 0006). `entrenar` vuelve a
ajustar el modelo con los pozos que cumplieron 12 meses desde la última corrida y lo promueve
a `champion` solo si le gana al baseline; si no le gana, termina con error y `predecir` no
corre, así el lake nunca se llena de predicciones de un modelo que no se validó.

**Cuándo.** El día 2 a las 7, veinticinco horas después de `gold_mensual` (día 1 a las 6).
Mismo criterio que ese DAG: se espera por calendario y no con `ExternalTaskSensor`, porque el
mart que leen las dos tareas es una tabla completa y no un incremento; si gold se atrasa, el
modelo se entrena con el mart del mes anterior, que es un resultado viejo pero correcto.

**MLflow.** `runner.py` reenvía al runner solo las variables de `FORWARDED_ENV`, y
`MLFLOW_TRACKING_URI` no está en esa lista. `runner_task` no acepta variables extra, así que
la URL va delante del comando: el runner corre con `bash -c`, donde `VAR=valor comando` es
una asignación válida. Es una línea por tarea y evita tocar una pieza compartida por todos
los DAGs para el caso de uso de uno solo.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from alertas import avisar_falla
from runner import runner_task

# El tracking server del perfil `mlflow` del compose, visto desde la red de Podman.
MLFLOW = "MLFLOW_TRACKING_URI=http://mlflow:5000"

with DAG(
    dag_id="ml_mensual",
    description="Entrena el modelo de producción a 12 meses y predice para todos los pozos",
    # El día 2 a las 7, después de gold_mensual.
    schedule="0 7 2 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "on_failure_callback": avisar_falla},
    tags=["ml", "mlflow", "gold"],
    doc_md=__doc__,
) as dag:
    # `python3` pelado y no `spark-submit`: el entrenamiento son 351 pozos en pandas, no
    # necesita una JVM. El runner se usa igual porque es el único lugar del proyecto donde
    # corre nuestro código (ADR 0006) y ya tiene el volumen con las dependencias instaladas.
    entrenar = runner_task("entrenar", f"{MLFLOW} python3 -m pipelines.ml.entrenar")

    predecir = runner_task("predecir", f"{MLFLOW} python3 -m pipelines.ml.predecir")

    entrenar >> predecir

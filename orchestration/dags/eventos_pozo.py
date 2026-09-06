"""DAG diario del clasificador de eventos de pozo: reentrenar y volver a detectar.

Dos tareas en serie en el mismo runner que el resto del stack (ADR 0006). `entrenar_eventos`
rearma el dataset de ventanas con las instancias de 3W que haya en landing y promueve el modelo
a `champion` solo si le gana a los baselines; si no le gana, termina con error y
`detectar_eventos` no corre, así el lake nunca se llena de alertas de un modelo que no se
validó. Es el mismo contrato que `ml_mensual` (ADR 0012), con otra cadencia.

**Cuándo.** Diario, y no mensual como el modelo de completación, porque la entrada es otra: la
telemetría llega a 1 Hz y `detectar_eventos` mira las últimas 24 horas de `event_time` de
`lake.bronze.telemetria_pozo`. Reentrenar todos los días es más de lo que el dataset necesita
—las instancias de landing cambian solo cuando alguien corre `fetch_3w`— pero mantiene las dos
tareas en el mismo DAG y el entrenamiento entero tarda diez minutos; separarlas sería dos DAGs
para ahorrar eso.

**Por qué esto no es streaming.** La alerta sale con horas de retraso y no con segundos. La
inferencia en línea sería un `foreachBatch` dentro del consumidor de Kafka; el motivo por el
que no está implementada está escrito en el módulo `pipelines/ml/detectar_eventos.py` y en el
ADR 0013.

**MLflow.** `runner.py` reenvía al runner solo las variables de `FORWARDED_ENV`, y
`MLFLOW_TRACKING_URI` no está en esa lista. `runner_task` no acepta variables extra, así que la
URL va delante del comando: el runner corre con `bash -c`, donde `VAR=valor comando` es una
asignación válida. Mismo criterio que `ml_mensual`.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from alertas import avisar_falla
from runner import runner_task

# El tracking server del perfil `mlflow` del compose, visto desde la red de Podman.
MLFLOW = "MLFLOW_TRACKING_URI=http://mlflow:5000"

with DAG(
    dag_id="eventos_pozo",
    description="Entrena el clasificador de eventos de pozo y detecta eventos en la telemetría",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "on_failure_callback": avisar_falla},
    tags=["ml", "mlflow", "streaming", "gold"],
    doc_md=__doc__,
) as dag:
    # `python3` pelado y no `spark-submit`: son 90.000 ventanas en pandas y scikit-learn, no
    # necesita una JVM. El runner se usa igual porque es el único lugar del proyecto donde
    # corre nuestro código (ADR 0006) y ya tiene las dependencias de ML instaladas.
    entrenar = runner_task("entrenar_eventos", f"{MLFLOW} python3 -m pipelines.ml.entrenar_eventos")

    detectar = runner_task(
        "detectar_eventos", f"{MLFLOW} python3 -m pipelines.ml.detectar_eventos --horas 24"
    )

    entrenar >> detectar

"""DAG diario de observabilidad: reconstruye el mart de salud y chequea la frescura.

No agrega ningún servicio: las dos tareas son `dbt` en el runner de siempre (ADR 0006, 0009).

- `salud`: `dbt build --select monitoreo` deja al día `gold.salud_pipeline` y
  `gold.calidad_por_corrida`, y corre sus tests.
- `frescura`: `dbt source freshness` compara la última carga de cada fuente contra los umbrales
  declarados en `pipelines/dbt/models/sources.yml` y **termina con código distinto de cero** si
  alguna pasó su `error_after`. Es la tarea que hace fallar el DAG y dispara el aviso.

**Por qué `salud` va primero.** El mart es un dato que conviene tener escrito todos los días,
sobre todo el día que algo se atrasó: si el chequeo de frescura fuera primero y fallara, la
tarea que escribe la foto del pipeline quedaría en `skipped` justo cuando hace falta mirarla.
El orden inverso deja siempre la tabla al día y usa la frescura como el control que corta.

En serie y no en paralelo por RAM: cada tarea levanta un driver de Spark y en la máquina de
desarrollo no entran dos (mismo motivo que en `produccion_pozo_mensual`).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from alertas import avisar_falla
from runner import runner_task

# 3 GB y no los 4 de `spark-defaults.conf`: con Airflow en la misma máquina de Podman, un
# driver de 4 GB deja al contenedor sin RAM (ver el comentario de `gold_mensual`).
SPARK_SUBMIT = "/opt/spark/bin/spark-submit --conf spark.driver.memory=3g"

with DAG(
    dag_id="monitoreo_diario",
    description="Salud del pipeline y frescura de las fuentes (dbt, sin servicios nuevos)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    # Sin reintento: si una fuente está vieja, volver a preguntar da lo mismo.
    default_args={"retries": 0, "on_failure_callback": avisar_falla},
    tags=["monitoreo", "dbt", "gold"],
    doc_md=__doc__,
) as dag:
    salud = runner_task(
        "salud",
        f"{SPARK_SUBMIT} /app/pipelines/dbt/run_dbt.py build --select monitoreo",
    )
    frescura = runner_task(
        "frescura",
        f"{SPARK_SUBMIT} /app/pipelines/dbt/run_dbt.py source freshness",
    )

    salud >> frescura

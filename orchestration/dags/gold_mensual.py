"""DAG mensual de la capa gold: dbt build sobre silver.

Una sola tarea, porque el grafo de dependencias entre los ocho modelos ya lo resuelve dbt:
partirlo en tareas de Airflow sería mantener el mismo orden escrito dos veces.

**Cómo espera a las fuentes.** Por calendario, no con `ExternalTaskSensor`. Los DAGs de origen
no comparten schedule (`fractura_diaria` es diario y los otros dos mensuales), así que un sensor
necesitaría un `execution_date_fn` por cada uno para alinear intervalos que no coinciden: tres
funciones de fecha para expresar "corré después". Este DAG arranca a las 6 de la mañana del día
1 y los tres de origen a medianoche del mismo día; si alguno se atrasa, gold trabaja sobre la
silver del mes anterior, que es un resultado viejo pero correcto (los modelos leen la tabla
entera, no un incremento).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from alertas import avisar_falla
from runner import runner_task

with DAG(
    dag_id="gold_mensual",
    description="Capa gold (modelos dimensionales con dbt) sobre silver",
    # El día 1 a las 6, seis horas después de los DAGs de las fuentes.
    schedule="0 6 1 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "on_failure_callback": avisar_falla},
    tags=["gold", "dbt"],
    doc_md=__doc__,
) as dag:
    # `dbt build` corre modelos y tests en el orden del grafo y frena la rama si un test falla:
    # una tabla que no pasa sus tests no propaga el error a las que dependen de ella.
    #
    # Driver de 3 GB y no los 4 de `spark-defaults.conf`: cuando el job lo lanza Airflow, el
    # contenedor de Airflow vive en la misma máquina de Podman que el runner, y 4 GB de driver
    # más Airflow más MinIO no entran en la RAM de la VM. La primera corrida terminó con el
    # contenedor matado por el kernel (exit 137). Desde `scripts/dbt.ps1`, sin Airflow en el
    # medio, los 4 GB del archivo alcanzan; por eso el ajuste vive acá y no en el conf global.
    dbt_build = runner_task(
        "dbt_build",
        "/opt/spark/bin/spark-submit --conf spark.driver.memory=3g"
        " /app/pipelines/dbt/run_dbt.py build",
    )

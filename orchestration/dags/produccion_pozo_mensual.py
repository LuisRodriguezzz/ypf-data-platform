"""DAG mensual de producción por pozo: landing -> bronze -> silver."""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from alertas import avisar_falla
from runner import runner_task

with DAG(
    dag_id="produccion_pozo_mensual",
    description="Producción por pozo (DDJJ Secretaría de Energía) hasta silver",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "on_failure_callback": avisar_falla},
    tags=["produccion_pozo", "bronze", "silver"],
    doc_md=__doc__,
) as dag:
    # Lineal a propósito: las dos silver son independientes, pero cada runner levanta un driver
    # de Spark de 4 GB y la máquina tiene 16. En paralelo no ganan tiempo, se pelean la RAM.
    ingesta = runner_task(
        "ingesta_landing",
        "python3 -m pipelines.ingest.cli run --dataset produccion_pozo",
    )
    bronze = runner_task(
        "bronze_produccion_pozo",
        "/opt/spark/bin/spark-submit pipelines/spark_jobs/bronze_load.py --dataset produccion_pozo",
    )
    silver_produccion = runner_task(
        "silver_produccion_pozo",
        "/opt/spark/bin/spark-submit pipelines/spark_jobs/silver_load.py"
        " --contract produccion_pozo",
    )
    silver_padron = runner_task(
        "silver_pozo_primera_produccion",
        "/opt/spark/bin/spark-submit pipelines/spark_jobs/silver_load.py"
        " --contract pozo_primera_produccion",
    )

    ingesta >> bronze >> silver_produccion >> silver_padron

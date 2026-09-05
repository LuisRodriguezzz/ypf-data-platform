"""DAG diario de datos de fractura (Adjunto IV): landing -> bronze."""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from runner import runner_task

with DAG(
    dag_id="fractura_diaria",
    description="Fractura de pozos (Adjunto IV, actualización diaria) hasta bronze",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["fractura", "bronze"],
    doc_md=__doc__,
) as dag:
    # Todavía no hay contrato de datos para fractura, así que la cadena termina en bronze.
    ingesta = runner_task(
        "ingesta_landing",
        "python3 -m pipelines.ingest.cli run --dataset fractura",
    )
    bronze = runner_task(
        "bronze_fractura",
        "/opt/spark/bin/spark-submit pipelines/spark_jobs/bronze_load.py --dataset fractura",
    )

    ingesta >> bronze

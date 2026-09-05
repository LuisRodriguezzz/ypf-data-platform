"""DAG mensual de reservas: landing -> bronze -> silver."""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from runner import runner_task

with DAG(
    dag_id="reservas_mensual",
    description="Reservas y recursos por yacimiento (ZIP anual) hasta silver",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["reservas", "silver"],
    doc_md=__doc__,
) as dag:
    ingesta = runner_task(
        "ingesta_landing",
        "python3 -m pipelines.ingest.cli run --dataset reservas",
    )
    # Sin spark-submit: el ZIP pesa 400 KB y el trabajo es desarmar un cuadro de Excel, algo
    # que Spark no lee. Escribe la tabla Iceberg con pyiceberg (docs/fuentes/reservas.md).
    bronze = runner_task(
        "bronze_reservas",
        "python3 -m pipelines.reservas.bronze_load",
    )
    # Silver sí es el job de siempre: bronze quedó con la misma forma que la que escribe Spark.
    silver = runner_task(
        "silver_reservas",
        "/opt/spark/bin/spark-submit pipelines/spark_jobs/silver_load.py --contract reservas",
    )

    ingesta >> bronze >> silver

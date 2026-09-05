"""DAG mensual de reservas comprobadas: solo ingesta a landing."""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from runner import runner_task

with DAG(
    dag_id="reservas_mensual",
    description="Reservas comprobadas por cuenca (ZIP anual) hasta landing",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["reservas", "landing"],
    doc_md=__doc__,
) as dag:
    # Los ZIP anuales todavía no tienen tabla bronze: hay que descomprimirlos primero.
    runner_task(
        "ingesta_landing",
        "python3 -m pipelines.ingest.cli run --dataset reservas",
    )

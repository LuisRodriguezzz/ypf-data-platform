"""DAG diario de datos de fractura (Adjunto IV): landing -> bronze -> silver."""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from alertas import avisar_falla
from runner import runner_task

with DAG(
    dag_id="fractura_diaria",
    description="Fractura de pozos (Adjunto IV, actualización diaria) hasta silver",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "on_failure_callback": avisar_falla},
    tags=["fractura", "bronze", "silver"],
    doc_md=__doc__,
) as dag:
    ingesta = runner_task(
        "ingesta_landing",
        "python3 -m pipelines.ingest.cli run --dataset fractura",
    )
    bronze = runner_task(
        "bronze_fractura",
        "/opt/spark/bin/spark-submit pipelines/spark_jobs/bronze_load.py --dataset fractura",
    )
    # El recurso se republica entero cada día; si el sha256 no cambió, silver no hace nada.
    silver = runner_task(
        "silver_fractura",
        "/opt/spark/bin/spark-submit pipelines/spark_jobs/silver_load.py --contract fractura",
    )

    ingesta >> bronze >> silver

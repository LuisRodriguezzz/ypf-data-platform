"""Aviso de falla compartido por todos los DAGs.

Se engancha una sola vez, en el `default_args` de cada DAG, así cualquier tarea que falle
—ingesta, bronze, silver, dbt, ML— pasa por acá. Hoy escribe una línea de log; el día que haya
un canal real (correo, Slack, PagerDuty) se conecta en esta función y en ningún otro lado.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger("alertas")

# La misma URL con la que se entra a la UI (la define el compose).
BASE_URL = os.environ.get("AIRFLOW__API__BASE_URL", "http://localhost:8080")


def avisar_falla(context) -> None:
    """Deja en el log qué falló y el link para ir a verlo."""
    tarea = context["task_instance"]
    corrida = context["dag_run"]
    url = f"{BASE_URL}/dags/{tarea.dag_id}/runs/{corrida.run_id}/tasks/{tarea.task_id}"
    logger.error(
        "ALERTA | dag=%s | tarea=%s | corrida=%s | intento=%s | %s",
        tarea.dag_id,
        tarea.task_id,
        corrida.run_id,
        tarea.try_number,
        url,
    )

"""Lanza un comando nuestro en el contenedor runner (ADR 0006).

Airflow orquesta y no ejecuta lógica de negocio: cada tarea arranca el mismo contenedor que
usa `scripts/spark-submit.ps1`, con el repo montado, y muere al terminar. El equivalente en
AWS es un DAG que dispara un job de Glue.
"""

from __future__ import annotations

import os

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Coordenadas del entorno: las define el servicio `airflow` de infra/docker/compose.yaml.
IMAGE = os.environ["RUNNER_IMAGE"]
NETWORK = os.environ["RUNNER_NETWORK"]
IVY_VOLUME = os.environ["RUNNER_IVY_VOLUME"]
# El motor de Podman resuelve los bind mounts en su propia máquina (WSL), no en Windows.
REPO_DIR = os.environ["RUNNER_REPO_DIR"]

# Credenciales y endpoints del lakehouse: llegan al contenedor de Airflow desde el compose y se
# reenvían tal cual, así no hay secretos escritos en los DAGs.
FORWARDED_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_REGION",
    "S3_LANDING_BUCKET",
    "ICEBERG_CATALOG_URI",
    "ICEBERG_WAREHOUSE",
    "POSTGRES_DSN",
)

MOUNTS = [
    Mount(source=f"{REPO_DIR}/pipelines", target="/app/pipelines", type="bind", read_only=True),
    Mount(source=f"{REPO_DIR}/config", target="/app/config", type="bind", read_only=True),
    # Los jars de Iceberg hay que declararlos antes de que arranque la JVM (ADR 0004).
    Mount(
        source=f"{REPO_DIR}/infra/docker/spark-defaults.conf",
        target="/opt/spark/conf/spark-defaults.conf",
        type="bind",
        read_only=True,
    ),
    # Cache de jars de Maven y de los paquetes de pip: sin esto cada tarea los baja de nuevo.
    Mount(source=IVY_VOLUME, target="/home/spark", type="volume"),
]


def runner_task(task_id: str, command: str) -> DockerOperator:
    """Tarea que corre `command` dentro del runner, con las dependencias ya instaladas."""
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        # pip resuelve en segundos cuando el volumen ya tiene los paquetes (ADR 0004).
        command=[
            "bash",
            "-c",
            "python3 -m pip install --user --quiet --disable-pip-version-check "
            f"-r /app/pipelines/spark_jobs/requirements-runner.txt && {command}",
        ],
        # Socket de la API de Podman montado por el compose (ver el volumen `podman-socket`).
        docker_url="unix:///var/run/podman/podman.sock",
        api_version="auto",
        network_mode=NETWORK,
        mounts=MOUNTS,
        mount_tmp_dir=False,
        working_dir="/app",
        environment={
            # La imagen trae HOME=/nonexistent y no pone /app en el path de módulos.
            "HOME": "/home/spark",
            "PYTHONPATH": "/app",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONIOENCODING": "utf-8",
            **{name: os.environ[name] for name in FORWARDED_ENV},
        },
        auto_remove="success",
    )

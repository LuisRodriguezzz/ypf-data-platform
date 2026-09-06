"""Coordenadas del MLflow del proyecto, compartidas por el entrenamiento y la inferencia.

Es un módulo chico a propósito: son los únicos datos que el entrenamiento y la inferencia
tienen que tener idénticos para que `predecir` cargue el modelo que dejó `entrenar`.
"""

from __future__ import annotations

import os

from pipelines.spark_jobs.config import DEFAULT_ENV_FILE, LakehouseConfig, read_env_file

EXPERIMENTO = "completacion_produccion"
MODELO = "completacion_produccion_12m"
# El alias reemplaza a los stages, que MLflow 3 ya no usa: `models:/<modelo>@champion` siempre
# apunta a la última versión que superó al baseline.
ALIAS = "champion"

URI_POR_DEFECTO = "http://localhost:5000"


def tracking_uri() -> str:
    """El server de MLflow: entorno primero, `config/local.env` después.

    Mismo criterio que `spark_jobs/config.py`. No se agrega a `LakehouseConfig` porque esa
    dataclass la leen los jobs de Spark, que no saben nada de MLflow.
    """
    del_entorno = os.environ.get("MLFLOW_TRACKING_URI")
    if del_entorno:
        return del_entorno
    return read_env_file(DEFAULT_ENV_FILE).get("MLFLOW_TRACKING_URI", URI_POR_DEFECTO)


def uri_champion() -> str:
    """URI del modelo en producción, tal como lo pide `mlflow.sklearn.load_model`."""
    return f"models:/{MODELO}@{ALIAS}"


def configurar_artefactos(config: LakehouseConfig) -> None:
    """Deja el entorno listo para que el cliente de MLflow lea y escriba en `s3://mlflow/`.

    El server guarda los artefactos en MinIO pero no hace de proxy: cada cliente resuelve el
    bucket por su cuenta, y el endpoint correcto depende de dónde corre (localhost:9000 desde
    el host, minio:9000 desde el runner). Es exactamente lo que ya resuelve `LakehouseConfig`,
    así que se reusa en vez de agregar tres variables más al compose.

    `setdefault` y no asignación: si alguien exporta las variables a mano, ganan las suyas.
    """
    for nombre, valor in (
        ("MLFLOW_S3_ENDPOINT_URL", config.s3_endpoint_url),
        ("AWS_ACCESS_KEY_ID", config.s3_access_key_id),
        ("AWS_SECRET_ACCESS_KEY", config.s3_secret_access_key),
        ("AWS_DEFAULT_REGION", config.s3_region),
    ):
        if valor:
            os.environ.setdefault(nombre, valor)

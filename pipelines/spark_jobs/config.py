"""Configuración de los jobs del lakehouse, leída del entorno.

No usa pydantic a propósito: la imagen del runner de Spark solo trae la stdlib y PySpark
(ver ADR 0004), así que este módulo tiene que funcionar sin dependencias. El `.env` del
repo se usa apenas como valor por defecto cuando la variable no está en el entorno.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# pipelines/spark_jobs/config.py -> raíz del repo
DEFAULT_ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "local.env"


@dataclass(frozen=True)
class LakehouseConfig:
    """Endpoints y credenciales que necesitan los jobs."""

    lakehouse_target: str  # local | aws
    s3_endpoint_url: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str
    s3_landing_bucket: str
    iceberg_catalog_uri: str
    iceberg_warehouse: str
    glue_warehouse: str
    postgres_dsn: str
    kafka_bootstrap_servers: str

    @property
    def is_aws(self) -> bool:
        return self.lakehouse_target == "aws"

    @property
    def s3_scheme(self) -> str:
        """Esquema de las rutas de landing: Glue trae su propio conector `s3://`."""
        return "s3" if self.is_aws else "s3a"


def read_env_file(path: Path) -> dict[str, str]:
    """Parsea `KEY=VALUE` de un .env; devuelve vacío si el archivo no existe."""
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def load_config(env_file: Path | str | None = None) -> LakehouseConfig:
    """Variables de entorno primero; el .env solo completa lo que falta."""
    override = env_file or os.environ.get("LAKEHOUSE_ENV_FILE")
    defaults = read_env_file(Path(override) if override else DEFAULT_ENV_FILE)

    def value(name: str, fallback: str) -> str:
        return os.environ.get(name) or defaults.get(name, fallback)

    return LakehouseConfig(
        lakehouse_target=value("LAKEHOUSE_TARGET", "local"),
        s3_endpoint_url=value("S3_ENDPOINT_URL", "http://localhost:9000"),
        s3_access_key_id=value("S3_ACCESS_KEY_ID", ""),
        s3_secret_access_key=value("S3_SECRET_ACCESS_KEY", ""),
        s3_region=value("S3_REGION", "us-east-1"),
        s3_landing_bucket=value("S3_LANDING_BUCKET", "landing"),
        iceberg_catalog_uri=value("ICEBERG_CATALOG_URI", "http://localhost:8181"),
        iceberg_warehouse=value("ICEBERG_WAREHOUSE", "s3://lakehouse/warehouse"),
        # Solo se usa con LAKEHOUSE_TARGET=aws: es el warehouse del Glue Data Catalog.
        glue_warehouse=value("GLUE_WAREHOUSE", ""),
        postgres_dsn=value("POSTGRES_DSN", ""),
        # Desde el host el broker se ve en localhost:29092; adentro del compose el servicio
        # `spark` recibe KAFKA_BOOTSTRAP_SERVERS=kafka:9092 y pisa este valor.
        kafka_bootstrap_servers=value("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    )

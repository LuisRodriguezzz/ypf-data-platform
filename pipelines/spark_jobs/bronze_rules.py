"""Reglas del job bronze que no dependen de Spark (se testean sin JVM)."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlparse

BOM = "\ufeff"


@dataclass(frozen=True)
class LandedFile:
    """Un archivo de landing listo para cargar, tal como lo describe el manifiesto."""

    resource_id: str
    resource_name: str
    landing_key: str
    sha256: str
    ingest_date: str  # ISO; Spark lo castea a date


def s3a_uri(bucket: str, key: str) -> str:
    """Ruta s3a del objeto de landing."""
    return f"s3a://{bucket}/{key.lstrip('/')}"


def clean_column_name(name: str) -> str:
    """Quita el BOM (los CSV del portal son UTF-8 con BOM) y espacios sobrantes."""
    return name.lstrip(BOM).strip()


def namespace_of(table: str) -> str:
    """`lake.bronze.produccion_pozo` -> `lake.bronze`."""
    return table.rsplit(".", 1)[0]


def pending_files(
    landed: list[LandedFile],
    loaded_sha256: dict[str, str],
) -> list[LandedFile]:
    """Recursos nuevos o cuyo sha256 cambió respecto de lo ya cargado en bronze."""
    return [file for file in landed if loaded_sha256.get(file.resource_id) != file.sha256]


def latest_ok_query(dataset: str) -> str:
    """Subconsulta JDBC con la última corrida `ok` de cada recurso del dataset."""
    # El dataset viene de un dict cerrado en el job, pero se valida igual porque
    # termina interpolado en SQL.
    if not dataset.replace("_", "").isalnum():
        raise ValueError(f"dataset invalido: {dataset}")
    return (
        "(SELECT DISTINCT ON (resource_id) "
        "resource_id, resource_name, landing_key, sha256, ingest_date "
        "FROM ingestion_manifest "
        f"WHERE dataset = '{dataset}' AND status = 'ok' "
        "ORDER BY resource_id, finished_at DESC, id DESC) AS ultimo_ok"
    )


def postgres_jdbc(dsn: str) -> tuple[str, dict[str, str]]:
    """`postgresql://user:pass@host:5432/db` -> URL JDBC y propiedades de conexión."""
    parsed = urlparse(dsn)
    url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}"
    properties = {
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "driver": "org.postgresql.Driver",
    }
    return url, properties

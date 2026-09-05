"""Reglas del job bronze que no dependen de Spark (se testean sin JVM)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import yaml

BOM = "\ufeff"
DEFAULT_TABLES_PATH = Path(__file__).with_name("bronze_tables.yaml")


def load_yaml_file(path: Path | str) -> Any:
    """Lee y parsea un archivo YAML (PyYAML, instalado en el runner por ADR 0004)."""
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


@dataclass(frozen=True)
class LandedFile:
    """Un archivo de landing listo para cargar, tal como lo describe el manifiesto."""

    resource_id: str
    resource_name: str
    landing_key: str
    sha256: str
    ingest_date: str  # ISO; Spark lo castea a date


@dataclass(frozen=True)
class TableRule:
    """Un patr\u00f3n de nombre de recurso y la tabla bronze a la que va."""

    match: str
    table: str


def s3a_uri(bucket: str, key: str) -> str:
    """Ruta s3a del objeto de landing."""
    return f"s3a://{bucket}/{key.lstrip('/')}"


def clean_column_name(name: str) -> str:
    """Quita el BOM (los CSV del portal son UTF-8 con BOM) y espacios sobrantes."""
    return name.lstrip(BOM).strip()


def namespace_of(table: str) -> str:
    """`lake.bronze.produccion_pozo` -> `lake.bronze`."""
    return table.rsplit(".", 1)[0]


def dataset_names(path: Path | str | None = None) -> list[str]:
    """Datasets declarados en bronze_tables.yaml, para las opciones de la CLI."""
    raw = load_yaml_file(path or DEFAULT_TABLES_PATH)
    return sorted(raw.get("datasets") or {})


def load_table_rules(dataset: str, path: Path | str | None = None) -> list[TableRule]:
    """Reglas de un dataset, en orden de evaluación."""
    raw = load_yaml_file(path or DEFAULT_TABLES_PATH)
    entries = (raw.get("datasets") or {}).get(dataset)
    if not entries:
        raise KeyError(f"dataset {dataset!r} no tiene tablas declaradas en bronze_tables.yaml")
    return [TableRule(match=entry["match"], table=entry["table"]) for entry in entries]


def table_for_resource(rules: list[TableRule], resource_name: str) -> str | None:
    """Tabla del primer patrón que coincide; None si el recurso no está mapeado."""
    for rule in rules:
        if re.search(rule.match, resource_name, re.IGNORECASE):
            return rule.table
    return None


def resources_by_table(
    landed: list[LandedFile],
    rules: list[TableRule],
) -> dict[str, list[LandedFile]]:
    """Agrupa los recursos por tabla destino, salteando los que no matchean."""
    groups: dict[str, list[LandedFile]] = {}
    for file in landed:
        table = table_for_resource(rules, file.resource_name)
        if table:
            groups.setdefault(table, []).append(file)
    return groups


def unmapped_resources(landed: list[LandedFile], rules: list[TableRule]) -> list[LandedFile]:
    """Recursos que no coinciden con ningún patrón: se loguean y no se cargan."""
    return [file for file in landed if table_for_resource(rules, file.resource_name) is None]


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

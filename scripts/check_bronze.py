"""Chequeo de la capa bronze desde el host, sin Spark ni Java.

Habla directo con el catálogo Iceberg REST y con MinIO usando pyiceberg.
Uso: uv run python scripts/check_bronze.py
"""

from __future__ import annotations

from datetime import UTC

from pyiceberg.catalog.rest import RestCatalog

from pipelines.spark_jobs.config import LakehouseConfig, load_config

NAMESPACE = "bronze"
TABLE = "produccion_pozo"


def open_catalog(config: LakehouseConfig) -> RestCatalog:
    """Catálogo REST apuntando a MinIO por su endpoint local."""
    return RestCatalog(
        "lake",
        **{
            "uri": config.iceberg_catalog_uri,
            "warehouse": config.iceberg_warehouse,
            "s3.endpoint": config.s3_endpoint_url,
            "s3.access-key-id": config.s3_access_key_id,
            "s3.secret-access-key": config.s3_secret_access_key,
            "s3.region": config.s3_region,
        },
    )


def print_tables(catalog: RestCatalog) -> list[tuple[str, ...]]:
    """Lista las tablas del namespace bronze."""
    tables = catalog.list_tables(NAMESPACE)
    print(f"namespace {NAMESPACE}: {len(tables)} tabla(s)")
    for identifier in tables:
        print(f"  - {'.'.join(identifier)}")
    return tables


def print_rows_by_resource(catalog: RestCatalog) -> None:
    """Filas por `_resource_id` leídas de la metadata de particiones (no escanea datos)."""
    table = catalog.load_table(f"{NAMESPACE}.{TABLE}")
    print(f"\n{TABLE}: {len(table.schema().fields)} columnas")
    print("filas por _resource_id:")
    total = 0
    for row in table.inspect.partitions().to_pylist():
        resource_id = row["partition"]["_resource_id"]
        count = row["record_count"]
        total += count
        print(f"  {resource_id}  {count:>12,}  ({row['file_count']} archivo/s)")
    print(f"  {'total':<36}  {total:>12,}")

    print("snapshots:")
    for snapshot in table.inspect.snapshots().to_pylist():
        committed = snapshot["committed_at"].astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        # `summary` es un map de Arrow: llega como lista de pares (clave, valor).
        summary = dict(snapshot["summary"] or [])
        added = summary.get("added-records", "-")
        print(f"  {committed}Z  {snapshot['operation']:<9} +{added} filas")


def main() -> int:
    config = load_config()
    print(f"catálogo {config.iceberg_catalog_uri} · warehouse {config.iceberg_warehouse}")
    catalog = open_catalog(config)
    tables = print_tables(catalog)
    if (NAMESPACE, TABLE) in tables:
        print_rows_by_resource(catalog)
    else:
        print(f"\n{TABLE} todavía no existe: correr el job bronze_load")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Chequeo de una capa del lakehouse desde el host, sin Spark ni Java.

Habla directo con el catálogo Iceberg REST y con MinIO usando pyiceberg.
Uso: uv run python scripts/check_lake.py --namespace silver
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.table import Table

from pipelines.spark_jobs.config import LakehouseConfig, load_config

DQ_RUNS = "dq_runs"
REJECTS_SUFFIX = "_rejects"


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


def table_names(catalog: RestCatalog, namespace: str) -> list[str]:
    """Nombres de tabla del namespace, ordenados."""
    return sorted(identifier[-1] for identifier in catalog.list_tables(namespace))


def partition_label(row: dict) -> str:
    """`{'anio': 2024}` -> `anio=2024`. Una tabla sin particiones no trae claves."""
    values = row.get("partition") or {}
    return ", ".join(f"{key}={value}" for key, value in values.items()) or "(sin particion)"


def print_partitions(table: Table, name: str) -> None:
    """Filas por partición leídas de la metadata (no escanea los datos)."""
    rows = table.inspect.partitions().to_pylist()
    columns = len(table.schema().fields)
    print(f"\n{name}: {columns} columnas · {len(rows)} particion(es)")
    total = 0
    for row in sorted(rows, key=partition_label):
        total += row["record_count"]
        archivos = f"({row['file_count']} arch.)"
        print(f"  {partition_label(row):<50}{row['record_count']:>12,}  {archivos}")
    print(f"  {'total':<50}{total:>12,}")


def print_last_snapshot(table: Table) -> None:
    """Última escritura de la tabla, para saber si el job corrió."""
    snapshots = table.inspect.snapshots().to_pylist()
    if not snapshots:
        return
    last = snapshots[-1]
    committed = last["committed_at"].astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    # `summary` es un map de Arrow: llega como lista de pares (clave, valor).
    summary = dict(last["summary"] or [])
    agregadas = summary.get("added-records", "-")
    print(f"  ultimo snapshot: {committed}Z {last['operation']} +{agregadas} filas")


def print_dq_runs(table: Table, limit: int = 10) -> None:
    """Historial de calidad: las últimas corridas registradas por el job silver."""
    runs = table.scan().to_arrow().to_pylist()
    runs.sort(key=lambda run: run["run_at"])
    print(f"\ndq_runs: {len(runs)} corrida(s), ultimas {min(limit, len(runs))}")
    encabezado = f"{'run_at':<20}{'contrato':<26}{'recurso':<14}{'in':>10}{'out':>10}{'rech':>7}"
    print(f"  {encabezado}  estado")
    for run in runs[-limit:]:
        momento = run["run_at"].astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"  {momento:<20}{run['contract']:<26}{run['resource_id'][:12]:<14}"
            f"{run['rows_in']:>10,}{run['rows_out']:>10,}{run['rows_rejected']:>7,}"
            f"  {run['status']}{'  ' + run['hard_failures'] if run['hard_failures'] else ''}"
        )


def print_rejects(table: Table, name: str) -> None:
    """Cuarentena agrupada por motivo: qué regla del contrato se está violando."""
    reasons = table.scan(selected_fields=("reject_reason",)).to_arrow().column(0).to_pylist()
    print(f"\n{name}: {len(reasons)} fila(s) en cuarentena")
    for reason, count in sorted(Counter(reasons).items(), key=lambda item: -item[1]):
        print(f"  {count:>6,}  {reason}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Muestra el estado de una capa del lakehouse")
    parser.add_argument("--namespace", default="bronze", help="bronze, silver o gold")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    print(f"catálogo {config.iceberg_catalog_uri} · warehouse {config.iceberg_warehouse}")
    catalog = open_catalog(config)

    names = table_names(catalog, args.namespace)
    print(f"namespace {args.namespace}: {len(names)} tabla(s)")
    for name in names:
        print(f"  - {name}")

    for name in names:
        table = catalog.load_table(f"{args.namespace}.{name}")
        if name == DQ_RUNS:
            print_dq_runs(table)
        elif name.endswith(REJECTS_SUFFIX):
            print_rejects(table, name)
        else:
            print_partitions(table, name)
            print_last_snapshot(table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

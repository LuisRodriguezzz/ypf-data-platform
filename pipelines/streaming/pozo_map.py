"""Mapeo de los pozos de 3W a pozos reales del upstream argentino.

La telemetria es REAL: son pozos de Petrobras (dataset 3W, CC BY 4.0). El pozo argentino al
que se la asocia es FICTICIO: se eligen los primeros `--pozos` idpozo no convencionales de
la cuenca Neuquina con produccion en el ultimo anio declarado y se reparten entre los pozos
de 3W en orden. Por eso la tabla lleva `data_origin = 'simulated'`: nadie tiene que poder
confundir esta serie con telemetria de un pozo de YPF.

Se escribe con pyiceberg y no con Spark: son 18 filas, no justifica levantar una JVM (mismo
criterio que el bronze de reservas).

Uso: uv run python -m pipelines.streaming.pozo_map
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, EqualTo, GreaterThan
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestampType

from pipelines.ingest.manifest import Manifest
from pipelines.spark_jobs.config import LakehouseConfig, load_config
from pipelines.streaming.eventos import mapear_pozos
from pipelines.streaming.landing_3w import archivos_en_landing

logger = logging.getLogger("streaming.pozo_map")

ORIGEN = "silver.produccion_pozo"
TABLA = "bronze.pozo_map_3w"
NAMESPACE = "bronze"
DATA_ORIGIN = "simulated"

# Los 13 equipos concurrentes que reporta el RTIC, el mismo numero de particiones que tiene
# el topic. La clave del mensaje es el idpozo, asi que un pozo siempre cae en la misma
# particion (varios pozos pueden compartirla: el hash no reparte uno por uno).
POZOS_POR_DEFECTO = 13
CUENCA = "NEUQUINA"
TIPO_RECURSO = "NO CONVENCIONAL"
EMPRESA = "YPF S.A."


def esquema() -> Schema:
    return Schema(
        NestedField(1, "well_3w", StringType(), required=False),
        NestedField(2, "idpozo", LongType(), required=False),
        NestedField(3, "empresa", StringType(), required=False),
        NestedField(4, "areayacimiento", StringType(), required=False),
        NestedField(5, "cuenca", StringType(), required=False),
        NestedField(6, "tipo_de_recurso", StringType(), required=False),
        NestedField(7, "anio_referencia", IntegerType(), required=False),
        NestedField(8, "data_origin", StringType(), required=False),
        NestedField(9, "_loaded_at", TimestampType(), required=False),
    )


def open_catalog(config: LakehouseConfig) -> RestCatalog:
    """Catalogo REST con las credenciales de MinIO (igual que `scripts/check_lake.py`)."""
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


def ultimo_anio(catalog: RestCatalog) -> int:
    """Anio mas reciente cargado en silver, leido de las particiones (no escanea datos)."""
    particiones = catalog.load_table(ORIGEN).inspect.partitions().to_pylist()
    return max(fila["partition"]["anio"] for fila in particiones)


def pozos_candidatos(catalog: RestCatalog, anio: int, cantidad: int) -> list[dict]:
    """Primeros `cantidad` idpozo no convencionales de la Neuquina con produccion en `anio`."""
    filtro = And(
        And(EqualTo("anio", anio), EqualTo("cuenca", CUENCA)),
        And(EqualTo("tipo_de_recurso", TIPO_RECURSO), EqualTo("empresa", EMPRESA)),
    )
    scan = catalog.load_table(ORIGEN).scan(
        row_filter=And(filtro, GreaterThan("prod_pet", 0.0)),
        selected_fields=("idpozo", "empresa", "areayacimiento", "cuenca", "tipo_de_recurso"),
    )
    # Un pozo aparece una vez por mes: se queda la primera fila de cada idpozo.
    unicos: dict[int, dict] = {}
    for fila in scan.to_arrow().to_pylist():
        unicos.setdefault(fila["idpozo"], fila)
    return [unicos[idpozo] for idpozo in sorted(unicos)[:cantidad]]


def filas_del_mapeo(wells_3w: list[str], candidatos: list[dict], anio: int) -> list[dict]:
    """Una fila por pozo de 3W con el pozo argentino que le toca."""
    destino = {fila["idpozo"]: fila for fila in candidatos}
    asignacion = mapear_pozos(wells_3w, sorted(destino))
    # timezone.utc y no datetime.UTC: este modulo tambien puede correr en el runner (3.10).
    ahora = datetime.now(timezone.utc).replace(tzinfo=None)
    return [
        {
            "well_3w": well,
            "idpozo": idpozo,
            "empresa": destino[idpozo]["empresa"],
            "areayacimiento": destino[idpozo]["areayacimiento"],
            "cuenca": destino[idpozo]["cuenca"],
            "tipo_de_recurso": destino[idpozo]["tipo_de_recurso"],
            "anio_referencia": anio,
            "data_origin": DATA_ORIGIN,
            "_loaded_at": ahora,
        }
        for well, idpozo in sorted(asignacion.items())
    ]


def escribir(catalog: RestCatalog, filas: list[dict]) -> None:
    """Reemplaza la tabla entera: el mapeo es determinista, no se acumulan versiones."""
    catalog.create_namespace_if_not_exists(NAMESPACE)
    existia = catalog.table_exists(TABLA)
    tabla = catalog.create_table_if_not_exists(TABLA, schema=esquema())
    arrow = pa.Table.from_pylist(filas, schema=tabla.schema().as_arrow())
    # `overwrite` sobre una tabla vacia avisa por warning que no borro nada: la primera vez
    # se hace append (mismo criterio que el bronze de reservas).
    if existia:
        tabla.overwrite(arrow)
    else:
        tabla.append(arrow)


def leer_mapeo(catalog: RestCatalog) -> dict[str, int]:
    """`{'WELL-00002': 72232, ...}` desde la tabla; lo usa el productor de replay."""
    filas = catalog.load_table(TABLA).scan(selected_fields=("well_3w", "idpozo")).to_arrow()
    return dict(zip(filas["well_3w"].to_pylist(), filas["idpozo"].to_pylist(), strict=True))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mapea los pozos de 3W a pozos reales")
    parser.add_argument("--pozos", type=int, default=POZOS_POR_DEFECTO, help="pozos destino")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    config = load_config()
    catalog = open_catalog(config)

    archivos = archivos_en_landing(Manifest(config.postgres_dsn))
    wells = sorted({archivo.well_3w for archivo in archivos})
    if not wells:
        logger.error("no hay archivos de 3W en landing: correr pipelines.streaming.fetch_3w")
        return 1

    anio = ultimo_anio(catalog)
    candidatos = pozos_candidatos(catalog, anio, args.pozos)
    filas = filas_del_mapeo(wells, candidatos, anio)
    escribir(catalog, filas)

    logger.info(
        "lake.%s: %d pozos de 3W -> %d pozos %s de la %s (anio %d)",
        TABLA,
        len(filas),
        len(candidatos),
        TIPO_RECURSO.lower(),
        CUENCA.title(),
        anio,
    )
    for fila in filas:
        logger.info("  %s -> %s (%s)", fila["well_3w"], fila["idpozo"], fila["areayacimiento"])
    return 0


if __name__ == "__main__":
    sys.exit(main())

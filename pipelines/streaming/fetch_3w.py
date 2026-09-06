"""Trae a landing un subconjunto de los Parquet reales del dataset 3W de Petrobras.

Los archivos se listan con la API de GitHub (no hay portal CKAN detras) y se suben a
`landing` en streaming, sin tocar el disco. Cada archivo deja una fila en
`ingestion_manifest` con dataset `telemetria_3w`, igual que el resto de las fuentes, y el
replay despues lee de ahi que hay para reproducir.

Solo se bajan los archivos `WELL-*`, que son las instancias reales; `SIMULATED_*` y
`DRAWN_*` son sinteticas y no aportan al objetivo (telemetria real de pozos).

Uso: uv run python -m pipelines.streaming.fetch_3w --classes 0,2,7
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date

import requests

from pipelines.ingest.manifest import STATUS_OK, STATUS_UNCHANGED, Manifest
from pipelines.ingest.settings import load_settings
from pipelines.ingest.storage import LandingStorage
from pipelines.streaming.eventos import elegir_archivos

logger = logging.getLogger("streaming.fetch_3w")

DATASET = "telemetria_3w"
REPO = "petrobras/3W"
API = f"https://api.github.com/repos/{REPO}/contents/dataset"
LANDING_PREFIX = "3w"
PREFIJO_REAL = "WELL-"
DESCARGA_CHUNK = 1024 * 1024
TIMEOUT = (10, 300)

# Cupo por clase: 0 normal, 2 cierre espurio de DHSV (las 22 instancias reales que existen)
# y 7 scaling en el PCK. Alcanza para un replay de 13 pozos simultaneos sin bajar 1,8 GB.
CUPO_POR_CLASE = {0: 10, 2: 22, 7: 10}


@dataclass(frozen=True)
class ArchivoRemoto:
    """Un Parquet del repo de 3W tal como lo describe la API de GitHub."""

    clase: int
    nombre: str
    url: str
    size: int
    sha: str

    @property
    def resource_id(self) -> str:
        """`2/WELL-00002_20131104004101.parquet`: unico y estable dentro del dataset."""
        return f"{self.clase}/{self.nombre}"

    def landing_key(self, prefijo_bucket: str = "") -> str:
        """`3w/class=2/WELL-...parquet`, bajo el prefijo del bucket (vacio en local)."""
        key = f"{LANDING_PREFIX}/class={self.clase}/{self.nombre}"
        return f"{prefijo_bucket}/{key}" if prefijo_bucket else key


def listar_clase(clase: int, cupo: int, session: requests.Session) -> list[ArchivoRemoto]:
    """Archivos reales de una clase, repartidos entre pozos distintos hasta el cupo."""
    respuesta = session.get(f"{API}/{clase}", timeout=TIMEOUT)
    respuesta.raise_for_status()
    items = {
        item["name"]: item
        for item in respuesta.json()
        if item["type"] == "file" and item["name"].startswith(PREFIJO_REAL)
    }
    nombres = elegir_archivos(list(items), cupo)
    logger.info("clase %d: %d archivos reales, %d elegidos", clase, len(items), len(nombres))
    return [
        ArchivoRemoto(
            clase=clase,
            nombre=nombre,
            url=items[nombre]["download_url"],
            size=items[nombre]["size"],
            sha=items[nombre]["sha"],
        )
        for nombre in nombres
    ]


def descargar(session: requests.Session, url: str) -> Iterator[bytes]:
    """GET en streaming: el contenido pasa a landing sin pasar por el disco."""
    with session.get(url, stream=True, timeout=TIMEOUT) as respuesta:
        respuesta.raise_for_status()
        yield from respuesta.iter_content(chunk_size=DESCARGA_CHUNK)


def ya_esta(previa: dict | None, sha: str, key: str, storage: LandingStorage) -> bool:
    """True si el mismo blob de git ya esta en landing (se guarda el sha en el manifiesto)."""
    if not previa or previa["last_modified_source"] != sha:
        return False
    return storage.object_size(key) == previa["size_bytes_landed"]


def ingestar(
    archivo: ArchivoRemoto,
    manifest: Manifest,
    storage: LandingStorage,
    session: requests.Session,
    dia: date,
) -> str:
    """Sube un archivo a landing y devuelve el estado con el que quedo en el manifiesto."""
    key = archivo.landing_key(storage.prefix)
    previa = manifest.latest_ok(DATASET, archivo.resource_id)
    run_id = manifest.start(
        dataset=DATASET,
        source_type="http",
        resource_id=archivo.resource_id,
        resource_name=archivo.nombre,
        url=archivo.url,
        size_bytes_source=archivo.size,
        # El sha del blob de git es la version del archivo: GitHub no manda Last-Modified util.
        last_modified_source=archivo.sha,
        ingest_date=dia,
        landing_key=key,
    )
    if ya_esta(previa, archivo.sha, key, storage):
        manifest.finish_ok(
            run_id,
            sha256=previa["sha256"],
            size_bytes_landed=previa["size_bytes_landed"],
            landing_key=key,
            status=STATUS_UNCHANGED,
        )
        return STATUS_UNCHANGED
    try:
        resultado = storage.upload_stream(key, descargar(session, archivo.url))
    except Exception as exc:
        logger.error("fallo %s: %s", archivo.resource_id, exc)
        manifest.finish_failed(run_id, f"{type(exc).__name__}: {exc}")
        raise
    manifest.finish_ok(
        run_id,
        sha256=resultado.sha256,
        size_bytes_landed=resultado.size_bytes,
        landing_key=resultado.key,
    )
    return STATUS_OK


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baja telemetria real de 3W a landing")
    parser.add_argument("--classes", default="0,2,7", help="clases de 3W separadas por coma")
    parser.add_argument(
        "--max-files-per-class",
        type=int,
        help=f"archivos por clase; por defecto {CUPO_POR_CLASE}",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    clases = [int(parte) for parte in args.classes.split(",") if parte.strip()]
    settings = load_settings()
    storage = LandingStorage.from_settings(settings)
    manifest = Manifest(settings.postgres_dsn)
    session = requests.Session()
    dia = date.today()

    comenzo = time.monotonic()
    estados: list[str] = []
    bytes_totales = 0
    for clase in clases:
        cupo = args.max_files_per_class or CUPO_POR_CLASE.get(clase, 10)
        for archivo in listar_clase(clase, cupo, session):
            estado = ingestar(archivo, manifest, storage, session, dia)
            estados.append(estado)
            bytes_totales += archivo.size if estado == STATUS_OK else 0
            logger.info("%s %s (%s bytes)", estado, archivo.resource_id, f"{archivo.size:,}")
    logger.info(
        "resumen: %d archivos | %d nuevos | %d sin cambios | %s MB bajados | %.1f s",
        len(estados),
        estados.count(STATUS_OK),
        estados.count(STATUS_UNCHANGED),
        f"{bytes_totales / 1024 / 1024:.1f}",
        time.monotonic() - comenzo,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

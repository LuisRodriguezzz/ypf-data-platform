"""Que archivos de 3W hay en landing, segun el manifiesto de ingesta.

Lo usan el mapeo de pozos y el productor de replay: el manifiesto es la unica fuente de
verdad sobre lo que se bajo, igual que en bronze de reservas.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import desc, select

from pipelines.ingest.manifest import STATUS_OK, STATUS_UNCHANGED, Manifest, ingestion_manifest
from pipelines.streaming.eventos import pozo_de_archivo

DATASET = "telemetria_3w"


@dataclass(frozen=True)
class ArchivoLanding:
    """Un Parquet de 3W ya subido a landing."""

    resource_id: str  # `2/WELL-00002_20131104004101.parquet`
    nombre: str
    landing_key: str

    @property
    def clase(self) -> int:
        return int(self.resource_id.split("/", 1)[0])

    @property
    def well_3w(self) -> str:
        return pozo_de_archivo(self.nombre)


def archivos_en_landing(
    manifest: Manifest, clases: list[int] | None = None
) -> list[ArchivoLanding]:
    """Ultima corrida buena de cada archivo, ordenada por resource_id (orden estable)."""
    tabla = ingestion_manifest
    consulta = (
        select(tabla)
        .where(
            tabla.c.dataset == DATASET,
            tabla.c.status.in_((STATUS_OK, STATUS_UNCHANGED)),
        )
        .order_by(tabla.c.resource_id, desc(tabla.c.finished_at), desc(tabla.c.id))
    )
    ultimos: dict[str, ArchivoLanding] = {}
    with manifest.engine.connect() as conn:
        for fila in conn.execute(consulta).mappings():
            if fila["resource_id"] in ultimos or not fila["landing_key"]:
                continue
            ultimos[fila["resource_id"]] = ArchivoLanding(
                resource_id=fila["resource_id"],
                nombre=fila["resource_name"],
                landing_key=fila["landing_key"],
            )
    archivos = sorted(ultimos.values(), key=lambda archivo: archivo.resource_id)
    if clases is None:
        return archivos
    return [archivo for archivo in archivos if archivo.clase in clases]

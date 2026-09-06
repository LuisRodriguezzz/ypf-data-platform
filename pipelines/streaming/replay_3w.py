"""Productor: reproduce la telemetria 3W de landing hacia el topic `telemetria_pozo`.

Cada archivo de 3W es un pozo distinto reproducido en paralelo: se intercalan por su
posicion relativa al inicio del archivo, asi los N pozos avanzan juntos como si estuvieran
midiendo al mismo tiempo. La clave del mensaje es el idpozo, que es lo que decide la
particion de Kafka.

Tiempo de evento: los archivos de 3W son de anios distintos (2013 a 2019). Si se mandara el
timestamp original, el watermark de Spark (que avanza con el maximo visto) tiraria como
tardio todo lo que viniera de un archivo mas viejo. Por eso `event_time` se rebasea al
arranque del replay y el timestamp original viaja igual en `event_time_3w`. El tiempo de
evento avanza a 1 Hz por pozo (como la fuente); `--speed` solo comprime el reloj de pared,
asi que a `--speed 60` un minuto de reloj son 60 minutos de tiempo de evento.

Uso (host):   uv run python -m pipelines.streaming.replay_3w --speed 60 --run-for 600
Uso (runner): podman-compose -f infra/docker/compose.yaml --profile spark run --rm spark
              bash -c "python3 -m pip install --user -q -r
              /app/pipelines/spark_jobs/requirements-runner.txt &&
              python3 -m pipelines.streaming.replay_3w --speed 60 --run-for 600"
              El runner recibe KAFKA_BOOTSTRAP_SERVERS=kafka:9092 del compose; desde el host
              la config apunta a localhost:29092 (los dos listeners del broker).
"""

from __future__ import annotations

import argparse
import heapq
import io
import itertools
import json
import logging
import random
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from confluent_kafka import Producer

from pipelines.ingest.manifest import Manifest
from pipelines.spark_jobs.config import LakehouseConfig, load_config
from pipelines.streaming.eventos import (
    PlanTardios,
    Pozo,
    construir_evento,
    demora_tardia,
    elegir_archivos,
)
from pipelines.streaming.landing_3w import ArchivoLanding, archivos_en_landing
from pipelines.streaming.pozo_map import leer_mapeo, open_catalog

logger = logging.getLogger("streaming.replay_3w")

TOPIC = "telemetria_pozo"
ARCHIVOS_POR_DEFECTO = 13  # un pozo por particion del topic
LOTE_PARQUET = 4096  # filas por lote: el archivo no se carga entero en memoria
SEMILLA = 20260906
UMBRAL_SLEEP = 0.005  # dormir menos de 5 ms no sirve: el timer de Windows tiene ~15 ms


@dataclass
class Conteos:
    """Que hizo el productor, para poder cruzarlo con lo que quedo en bronze."""

    publicados: int = 0
    retenidos: int = 0  # eventos apartados para simular el corte de enlace
    tardios_publicados: int = 0


def abrir_s3(config: LakehouseConfig):
    """Cliente S3 apuntado a landing (MinIO en local)."""
    return boto3.client(
        "s3",
        endpoint_url=config.s3_endpoint_url or None,
        aws_access_key_id=config.s3_access_key_id or None,
        aws_secret_access_key=config.s3_secret_access_key or None,
        region_name=config.s3_region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if config.s3_endpoint_url else "auto"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def lecturas(cliente, bucket: str, archivo: ArchivoLanding, pozo: Pozo) -> Iterator[tuple]:
    """`(offset_en_segundos, pozo, fila)` de un archivo, en orden y de a lotes."""
    cuerpo = cliente.get_object(Bucket=bucket, Key=archivo.landing_key)["Body"].read()
    parquet = pq.ParquetFile(io.BytesIO(cuerpo))
    inicio: datetime | None = None
    for lote in parquet.iter_batches(batch_size=LOTE_PARQUET):
        for fila in lote.to_pylist():
            if inicio is None:
                inicio = fila["timestamp"]
            yield (fila["timestamp"] - inicio).total_seconds(), pozo, fila


def lecturas_en_bucle(cliente, bucket: str, archivo: ArchivoLanding, pozo: Pozo) -> Iterator[tuple]:
    """Igual que `lecturas` pero repite el archivo cuando se acaba: el pozo no deja de medir.

    Las instancias de 3W duran entre una y ocho horas; a 60x las mas cortas se agotan a los
    tres minutos y el caudal se desploma. Repetir mantiene los N pozos midiendo a 1 Hz.
    """
    corrido = 0.0
    while True:
        ultimo = 0.0
        for offset, pozo_, fila in lecturas(cliente, bucket, archivo, pozo):
            ultimo = offset
            yield corrido + offset, pozo_, fila
        corrido += ultimo + 1  # +1 s: la vuelta siguiente arranca en la muestra que sigue


def elegir_replay(archivos: list[ArchivoLanding], maximo: int) -> list[ArchivoLanding]:
    """Un archivo por pozo distinto hasta el maximo (reparto estable entre pozos)."""
    por_nombre = {archivo.nombre: archivo for archivo in archivos}
    return [por_nombre[nombre] for nombre in elegir_archivos(list(por_nombre), maximo)]


def esperar(inicio_wall: float, objetivo_s: float) -> None:
    """Frena hasta que el reloj de pared alcance el offset dividido por la velocidad."""
    retraso = objetivo_s - (time.monotonic() - inicio_wall)
    if retraso > UMBRAL_SLEEP:
        time.sleep(retraso)


def publicar(producer: Producer, evento: dict) -> None:
    """Manda el evento con el idpozo como clave: un pozo, siempre la misma particion."""
    datos = json.dumps(evento).encode("utf-8")
    while True:
        try:
            producer.produce(TOPIC, key=str(evento["idpozo"]).encode("utf-8"), value=datos)
            break
        except BufferError:
            # La cola local se lleno: se le da tiempo a librdkafka a drenarla.
            producer.poll(0.5)
    producer.poll(0)


def reproducir(
    producer: Producer,
    flujo: Iterator[tuple],
    base: datetime,
    plan: PlanTardios,
    speed: float,
    run_for: float,
) -> Conteos:
    """Bucle principal: publica en orden, reteniendo una fraccion para que llegue tarde."""
    conteos = Conteos()
    rng = random.Random(SEMILLA)
    pendientes: list[tuple[float, int, dict]] = []  # heap por offset de emision
    orden = itertools.count()  # desempate del heap: los dicts no se comparan
    inicio_wall = time.monotonic()
    ultimo_log = 0.0  # segundos transcurridos en el ultimo aviso de avance

    for offset, pozo, fila in flujo:
        esperar(inicio_wall, offset / speed)
        # Primero salen los tardios que ya vencieron: asi llegan despues de eventos con
        # tiempo de evento mayor, que es justo lo que tiene que aguantar el watermark.
        while pendientes and pendientes[0][0] <= offset:
            publicar(producer, heapq.heappop(pendientes)[2])
            conteos.tardios_publicados += 1
            conteos.publicados += 1

        evento = construir_evento(pozo, fila, base + timedelta(seconds=offset))
        demora = demora_tardia(rng, plan)
        if demora is None:
            publicar(producer, evento)
            conteos.publicados += 1
        else:
            heapq.heappush(pendientes, (offset + demora, next(orden), evento))
            conteos.retenidos += 1

        transcurrido = time.monotonic() - inicio_wall
        if transcurrido - ultimo_log >= 30:
            ultimo_log = transcurrido
            logger.info(
                "%.0f s | %s eventos | %s/s | %d en espera",
                transcurrido,
                f"{conteos.publicados:,}",
                f"{conteos.publicados / transcurrido:,.0f}",
                len(pendientes),
            )
        if run_for and transcurrido >= run_for:
            logger.info("corte por --run-for a los %.0f s", transcurrido)
            break

    # Lo que quedo retenido se manda igual: es lo que llegaria al restablecerse el enlace.
    for _, _, evento in sorted(pendientes):
        publicar(producer, evento)
        conteos.tardios_publicados += 1
        conteos.publicados += 1
    return conteos


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reproduce telemetria 3W hacia Kafka")
    parser.add_argument("--speed", type=float, default=60.0, help="1 = tiempo real (1 Hz/pozo)")
    parser.add_argument("--max-files", type=int, default=ARCHIVOS_POR_DEFECTO)
    parser.add_argument("--classes", default="", help="clases de 3W a reproducir (default: todas)")
    parser.add_argument("--late-fraction", type=float, default=0.05)
    # Duracion del corte de enlace, en segundos de tiempo de evento. Subirla por encima de
    # `watermark + trigger x speed` es lo que hace que Spark descarte los tardios (ADR 0011).
    parser.add_argument("--late-min", type=float, default=30.0)
    parser.add_argument("--late-max", type=float, default=120.0)
    parser.add_argument("--run-for", type=float, default=0, help="segundos de reloj; 0 = todo")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="repetir cada archivo al terminarlo (necesita --run-for para cortar)",
    )
    parser.add_argument("--bootstrap-servers", default="", help="default: config del entorno")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    config = load_config()
    clases = [int(parte) for parte in args.classes.split(",") if parte.strip()] or None

    archivos = elegir_replay(
        archivos_en_landing(Manifest(config.postgres_dsn), clases), args.max_files
    )
    if not archivos:
        logger.error("no hay archivos de 3W en landing: correr pipelines.streaming.fetch_3w")
        return 1
    mapeo = leer_mapeo(open_catalog(config))
    faltantes = {archivo.well_3w for archivo in archivos} - set(mapeo)
    if faltantes:
        logger.error("pozos sin mapear %s: correr pipelines.streaming.pozo_map", sorted(faltantes))
        return 1

    servidores = args.bootstrap_servers or config.kafka_bootstrap_servers
    # enable.idempotence: sin esto, un reintento de librdkafka sobre un mensaje que el broker
    # ya escribio deja el evento duplicado en el topic. Con idempotencia el broker descarta el
    # reenvio, que es lo que hace que "eventos publicados == filas en bronze" se sostenga.
    producer = Producer(
        {"bootstrap.servers": servidores, "linger.ms": 50, "enable.idempotence": True}
    )
    cliente = abrir_s3(config)
    leer = lecturas_en_bucle if args.loop else lecturas
    flujo = heapq.merge(
        *[
            leer(
                cliente,
                config.s3_landing_bucket,
                archivo,
                Pozo(archivo.well_3w, mapeo[archivo.well_3w]),
            )
            for archivo in archivos
        ],
        key=lambda lectura: lectura[0],
    )
    for archivo in archivos:
        logger.info(
            "  clase %d %s -> idpozo %s", archivo.clase, archivo.nombre, mapeo[archivo.well_3w]
        )
    # timezone.utc y no datetime.UTC: este modulo tambien corre en el runner (Python 3.10).
    base = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    logger.info(
        "replay a %sx | %d pozos | tardios %.0f%% de %.0f-%.0f s | broker %s | base %sZ",
        args.speed,
        len(archivos),
        args.late_fraction * 100,
        args.late_min,
        args.late_max,
        servidores,
        base.isoformat(),
    )

    comenzo = time.monotonic()
    plan = PlanTardios(fraccion=args.late_fraction, minimo_s=args.late_min, maximo_s=args.late_max)
    try:
        conteos = reproducir(producer, flujo, base, plan, args.speed, args.run_for)
    finally:
        producer.flush(30)
    duracion = time.monotonic() - comenzo
    logger.info(
        "publicados %s | retenidos como tardios %s (%.1f%%) | reenviados %s | %.0f s | %.0f ev/s",
        f"{conteos.publicados:,}",
        f"{conteos.retenidos:,}",
        100 * conteos.retenidos / max(conteos.publicados, 1),
        f"{conteos.tardios_publicados:,}",
        duracion,
        conteos.publicados / max(duracion, 1),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

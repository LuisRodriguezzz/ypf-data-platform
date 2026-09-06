"""Piezas puras del modulo de streaming: sensores, evento JSON y plan de tardios.

No importa Kafka ni Spark a proposito: esto lo comparten el productor (`replay_3w`) y el
consumidor (`consume_telemetria`), y asi se puede testear sin infraestructura. Tambien corre
en el runner, que trae Python 3.10 y solo la stdlib (ADR 0004).
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime

# Los 27 sensores del 3W v2.0.0, tal como se llaman en el Parquet (dataset.ini de Petrobras).
SENSORES_3W = (
    "ABER-CKGL",
    "ABER-CKP",
    "ESTADO-DHSV",
    "ESTADO-M1",
    "ESTADO-M2",
    "ESTADO-PXO",
    "ESTADO-SDV-GL",
    "ESTADO-SDV-P",
    "ESTADO-W1",
    "ESTADO-W2",
    "ESTADO-XO",
    "P-ANULAR",
    "P-JUS-BS",
    "P-JUS-CKGL",
    "P-JUS-CKP",
    "P-MON-CKGL",
    "P-MON-CKP",
    "P-MON-SDV-P",
    "P-PDG",
    "PT-P",
    "P-TPT",
    "QBS",
    "QGL",
    "T-JUS-CKP",
    "T-MON-CKP",
    "T-PDG",
    "T-TPT",
)

# Sensores que agrega silver: presion de fondo (PDG), presion y temperatura del TPT y presion
# aguas arriba del choke. Son los cuatro que estan poblados en casi todos los archivos y los
# que mueve un cierre espurio de DHSV (clase 2).
SENSORES_CLAVE = ("p_pdg", "p_tpt", "t_tpt", "p_mon_ckp")

# Formato del `event_time` que viaja en el JSON. UTC explicito para que el consumidor no
# dependa de la zona horaria de la maquina donde corre el productor.
FORMATO_TIEMPO = "%Y-%m-%dT%H:%M:%S.%f"


def nombre_columna(sensor: str) -> str:
    """`P-PDG` -> `p_pdg`: los guiones obligarian a backticks en cada consulta SQL."""
    return sensor.lower().replace("-", "_")


# Esquema del evento y de bronze, en un solo lugar: el productor emite estas claves y
# `consume_telemetria` arma con esto el StructType del `from_json`.
CAMPOS_EVENTO: tuple[tuple[str, str], ...] = (
    ("well_3w", "string"),  # pozo de Petrobras del que sale la lectura
    ("idpozo", "long"),  # pozo argentino al que se mapea (ficticio, ver pozo_map)
    ("event_time", "timestamp"),  # tiempo del evento en el replay
    ("event_time_3w", "string"),  # timestamp original del Parquet, para trazabilidad
    ("class", "int"),  # etiqueta 3W: 0 normal, N evento, 100+N transitorio
    ("state", "int"),
) + tuple((nombre_columna(sensor), "double") for sensor in SENSORES_3W)


# Tipo SQL de cada tipo del evento, para el DDL de la tabla bronze.
TIPOS_SQL = {
    "string": "string",
    "long": "bigint",
    "int": "int",
    "double": "double",
    "timestamp": "timestamp",
}


def columnas_sql() -> str:
    """`well_3w string, idpozo bigint, ...`: las columnas de bronze para el CREATE TABLE."""
    return ", ".join(f"{nombre} {TIPOS_SQL[tipo]}" for nombre, tipo in CAMPOS_EVENTO)


@dataclass(frozen=True)
class Pozo:
    """Un archivo de 3W en replay y el pozo real que representa."""

    well_3w: str
    idpozo: int


def formatear_tiempo(momento: datetime) -> str:
    """ISO en UTC con milisegundos: `2026-09-06T12:00:00.123Z`."""
    return momento.strftime(FORMATO_TIEMPO)[:-3] + "Z"


def _es_valor(valor: object) -> bool:
    """Descarta nulos y NaN: `json.dumps(nan)` escribe `NaN`, que no es JSON valido."""
    if valor is None:
        return False
    # NaN es el unico valor distinto de si mismo.
    return valor == valor


def construir_evento(pozo: Pozo, fila: dict, event_time: datetime) -> dict:
    """Evento de una lectura: identificacion, tiempos y los sensores que no son nulos.

    Los archivos viejos de 3W traen 23 de 27 sensores enteros en nulo (semana 0), asi que se
    omiten las claves sin valor en vez de mandar nulls: el JSON queda un 80 % mas chico y el
    esquema del consumidor las completa igual.
    """
    evento: dict = {
        "well_3w": pozo.well_3w,
        "idpozo": pozo.idpozo,
        "event_time": formatear_tiempo(event_time),
        "event_time_3w": fila["timestamp"].isoformat(),
    }
    for campo in ("class", "state"):
        if _es_valor(fila.get(campo)):
            evento[campo] = int(fila[campo])
    for sensor in SENSORES_3W:
        if _es_valor(fila.get(sensor)):
            evento[nombre_columna(sensor)] = float(fila[sensor])
    return evento


@dataclass(frozen=True)
class PlanTardios:
    """Corte del enlace satelital: que fraccion de eventos se retiene y cuanto.

    El RTIC recibe la telemetria por Starlink y los cortes hacen que parte de las lecturas
    lleguen minutos tarde. Se simula reteniendo una fraccion de los eventos y emitiendolos
    con una demora en *tiempo de evento*, que es contra lo que mide el watermark de Spark.
    """

    fraccion: float = 0.05
    minimo_s: float = 30.0
    maximo_s: float = 120.0


def demora_tardia(rng: random.Random, plan: PlanTardios) -> float | None:
    """Segundos que se retiene esta lectura, o None si sale al instante.

    Se consume siempre un `random()` (y solo a veces el segundo) para que la secuencia sea
    reproducible con una semilla fija.
    """
    if plan.fraccion <= 0 or rng.random() >= plan.fraccion:
        return None
    return rng.uniform(plan.minimo_s, plan.maximo_s)


def pozo_de_archivo(nombre: str) -> str:
    """`WELL-00002_20131104004101.parquet` -> `WELL-00002`."""
    return nombre.split("_", 1)[0]


def elegir_archivos(nombres: list[str], maximo: int) -> list[str]:
    """Reparte el cupo entre pozos distintos, en vez de tomar los primeros N por nombre.

    La clase 0 tiene 594 archivos de solo 9 pozos: ordenando por nombre, los primeros 10
    saldrian todos de WELL-00001. Se recorre pozo por pozo en rondas, que ademas es estable.
    """
    por_pozo: dict[str, list[str]] = {}
    for nombre in sorted(nombres):
        por_pozo.setdefault(pozo_de_archivo(nombre), []).append(nombre)

    elegidos: list[str] = []
    for ronda in range(max((len(archivos) for archivos in por_pozo.values()), default=0)):
        for pozo in sorted(por_pozo):
            if len(elegidos) >= maximo:
                return elegidos
            if len(por_pozo[pozo]) > ronda:
                elegidos.append(por_pozo[pozo][ronda])
    return elegidos


def mapear_pozos(wells_3w: list[str], idpozos: list[int]) -> dict[str, int]:
    """Asigna a cada pozo de 3W un idpozo real, en orden y ciclando si sobran pozos 3W.

    Determinista: mismas entradas, mismo mapeo. La telemetria es real (Petrobras) pero el
    pozo argentino al que se la asocia es ficticio; por eso la tabla lleva
    `data_origin = 'simulated'`.
    """
    if not idpozos:
        raise ValueError("no hay pozos destino para mapear")
    return {
        well: idpozos[indice % len(idpozos)] for indice, well in enumerate(sorted(set(wells_3w)))
    }

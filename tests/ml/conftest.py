"""Un mart de juguete con la misma forma que `gold.mart_pozo_completacion_produccion`.

Ocho pozos alcanzan para afirmar sobre filtros, topes y armado de tablas sin levantar MinIO.
Cada uno declara solo lo que lo hace distinto; el resto de las columnas sale de `BASE`.
"""

from __future__ import annotations

import pandas as pd
import pytest

BASE = {
    "tipo_de_recurso": "NO CONVENCIONAL",
    "sub_tipo_recurso": "SHALE",
    "meses_con_declaracion": 12.0,
    "cuenca": "NEUQUINA",
    "formacion": "vaca muerta",
    "tipo_terminacion": "Tapón disparo",
    "profundidad": 3000.0,
    "presion_maxima_psi": 10_000.0,
    "potencia_equipos_fractura_hp": 40_000.0,
    "co2_inyectado_m3": 0.0,
    "duracion_dias": 20.0,
}

# Cada pozo aporta un caso: presión fuera de rango, potencia fuera de rango, historia
# incompleta, pozo vertical (rama 0) y un convencional que el filtro tiene que sacar.
POZOS = (
    {"idpozo": 1, "areayacimiento": "LOMA CAMPANA", "rama": 2500.0, "etapas": 40, "pet": 30_000.0},
    {
        "idpozo": 2,
        "areayacimiento": "LOMA CAMPANA",
        "rama": 2000.0,
        "etapas": 30,
        "pet": 20_000.0,
        "presion_maxima_psi": 209_640.0,
    },
    {
        "idpozo": 3,
        "areayacimiento": "FORTIN DE PIEDRA",
        "rama": 1800.0,
        "etapas": 25,
        "pet": 15_000.0,
        "potencia_equipos_fractura_hp": 232_159.0,
    },
    {
        "idpozo": 4,
        "areayacimiento": "FORTIN DE PIEDRA",
        "rama": 1500.0,
        "etapas": 20,
        "pet": 4_000.0,
        "meses_con_declaracion": 6.0,
    },
    {
        "idpozo": 5,
        "areayacimiento": "AGUADA PICHANA",
        "rama": 0.0,
        "etapas": 10,
        "pet": 0.0,
        "formacion": "lajas",
        "sub_tipo_recurso": "TIGHT",
    },
    {
        "idpozo": 6,
        "areayacimiento": "AGUADA PICHANA",
        "rama": 0.0,
        "etapas": 8,
        "pet": 100.0,
        "formacion": "lajas",
        "sub_tipo_recurso": "TIGHT",
        "meses_con_declaracion": 3.0,
    },
    {
        "idpozo": 7,
        "areayacimiento": "EL TORDILLO",
        "rama": 0.0,
        "etapas": 2,
        "pet": 500.0,
        "tipo_de_recurso": "CONVENCIONAL",
        "formacion": "chubut",
    },
    {"idpozo": 8, "areayacimiento": "BANDURRIA SUR", "rama": 3000.0, "etapas": 50, "pet": 60_000.0},
)


def _fila(pozo: dict) -> dict:
    """Un pozo completo: la base, lo que el pozo pisa y lo que se deriva de rama y etapas."""
    etapas = float(pozo["etapas"])
    return {
        **BASE,
        **{clave: valor for clave, valor in pozo.items() if clave not in ("rama", "etapas", "pet")},
        "fecha_inicio_fractura": pd.Timestamp(f"20{18 + pozo['idpozo'] % 5}-04-15"),
        "longitud_rama_horizontal_m": pozo["rama"],
        "cantidad_fracturas": etapas,
        "arena_bombeada_total_tn": etapas * 300.0,
        "agua_inyectada_m3": etapas * 1200.0,
        "prod_pet_12m": pozo["pet"],
    }


@pytest.fixture
def mart() -> pd.DataFrame:
    """El mart de juguete, con las columnas que usa `pipelines.ml.datos`."""
    return pd.DataFrame([_fila(pozo) for pozo in POZOS])

"""Tests de las piezas puras del streaming (no requieren Kafka, Spark ni MinIO)."""

from __future__ import annotations

import json
import random
from datetime import datetime

import pytest

from pipelines.streaming.eventos import (
    CAMPOS_EVENTO,
    SENSORES_3W,
    SENSORES_CLAVE,
    PlanTardios,
    Pozo,
    columnas_sql,
    construir_evento,
    demora_tardia,
    elegir_archivos,
    formatear_tiempo,
    mapear_pozos,
    nombre_columna,
    pozo_de_archivo,
)
from pipelines.streaming.landing_3w import ArchivoLanding

POZO = Pozo(well_3w="WELL-00002", idpozo=72232)
MOMENTO = datetime(2026, 9, 6, 3, 10, 43)


def fila(**valores) -> dict:
    """Una lectura de 3W: todos los sensores en nulo salvo los que se pasen."""
    base = dict.fromkeys(SENSORES_3W)
    base.update({"timestamp": datetime(2013, 11, 4, 0, 41, 1), "class": None, "state": None})
    base.update(valores)
    return base


def test_nombre_columna_saca_los_guiones():
    assert nombre_columna("P-MON-CKP") == "p_mon_ckp"
    assert nombre_columna("QGL") == "qgl"


def test_los_sensores_clave_existen_en_3w():
    assert set(SENSORES_CLAVE) <= {nombre_columna(sensor) for sensor in SENSORES_3W}


def test_campos_evento_cubren_los_27_sensores_y_es_json_valido():
    nombres = [nombre for nombre, _ in CAMPOS_EVENTO]
    assert len(nombres) == len(set(nombres))
    assert nombres[:4] == ["well_3w", "idpozo", "event_time", "event_time_3w"]
    assert len([n for n, tipo in CAMPOS_EVENTO if tipo == "double"]) == len(SENSORES_3W)


def test_construir_evento_omite_nulos_y_nan():
    evento = construir_evento(POZO, fila(**{"P-PDG": 22450690.0, "P-TPT": float("nan")}), MOMENTO)
    assert evento["p_pdg"] == 22450690.0
    assert "p_tpt" not in evento  # NaN se descarta: json.dumps lo escribiria como `NaN`
    assert "qgl" not in evento
    assert "class" not in evento
    assert json.loads(json.dumps(evento))["idpozo"] == 72232


def test_construir_evento_lleva_los_dos_tiempos():
    evento = construir_evento(POZO, fila(**{"class": 2, "state": 0}), MOMENTO)
    assert evento["event_time"] == "2026-09-06T03:10:43.000Z"
    assert evento["event_time_3w"] == "2013-11-04T00:41:01"
    assert evento["class"] == 2
    assert evento["well_3w"] == "WELL-00002"


def test_el_evento_solo_trae_claves_del_esquema():
    """Lo que emite el productor tiene que entrar entero en el `from_json` del consumidor."""
    evento = construir_evento(POZO, fila(**{"P-PDG": 1.0, "class": 0, "state": 1}), MOMENTO)
    assert set(evento) <= {nombre for nombre, _ in CAMPOS_EVENTO}


def test_columnas_sql_arma_el_ddl_de_bronze():
    ddl = columnas_sql()
    assert ddl.startswith("well_3w string, idpozo bigint, event_time timestamp,")
    assert "p_mon_ckp double" in ddl
    assert "-" not in ddl  # un guion medio obligaria a backticks en cada consulta
    assert ddl.count(" double") == len(SENSORES_3W)


def test_formatear_tiempo_en_milisegundos():
    assert formatear_tiempo(datetime(2026, 1, 2, 3, 4, 5, 123456)) == "2026-01-02T03:04:05.123Z"


def test_demora_tardia_es_reproducible_con_semilla_fija():
    plan = PlanTardios(fraccion=0.05)
    demoras = [demora_tardia(random.Random(7), plan) for _ in range(3)]
    assert demoras == [demora_tardia(random.Random(7), plan)] * 3


def test_demora_tardia_respeta_la_fraccion_y_el_rango():
    plan = PlanTardios(fraccion=0.05)
    rng = random.Random(20260906)
    demoras = [demora_tardia(rng, plan) for _ in range(10_000)]
    tardios = [demora for demora in demoras if demora is not None]
    assert 400 < len(tardios) < 600  # ~5 % de 10.000
    assert all(30.0 <= demora <= 120.0 for demora in tardios)


def test_sin_tardios_cuando_la_fraccion_es_cero():
    rng = random.Random(1)
    assert all(demora_tardia(rng, PlanTardios(fraccion=0)) is None for _ in range(100))


def test_pozo_de_archivo():
    assert pozo_de_archivo("WELL-00002_20131104004101.parquet") == "WELL-00002"


def test_elegir_archivos_reparte_entre_pozos():
    nombres = [f"WELL-0000{pozo}_2017010{indice}.parquet" for pozo in (1, 2) for indice in range(5)]
    elegidos = elegir_archivos(nombres, 4)
    assert [pozo_de_archivo(nombre) for nombre in elegidos] == [
        "WELL-00001",
        "WELL-00002",
        "WELL-00001",
        "WELL-00002",
    ]


def test_elegir_archivos_es_estable_y_no_pasa_el_maximo():
    nombres = ["WELL-00003_b.parquet", "WELL-00001_a.parquet", "WELL-00002_a.parquet"]
    assert elegir_archivos(nombres, 2) == ["WELL-00001_a.parquet", "WELL-00002_a.parquet"]
    assert elegir_archivos(nombres, 99) == sorted(nombres)


def test_archivo_de_landing_saca_clase_y_pozo_del_resource_id():
    archivo = ArchivoLanding(
        resource_id="2/WELL-00002_20131104004101.parquet",
        nombre="WELL-00002_20131104004101.parquet",
        landing_key="3w/class=2/WELL-00002_20131104004101.parquet",
    )
    assert archivo.clase == 2
    assert archivo.well_3w == "WELL-00002"


def test_mapear_pozos_cicla_cuando_sobran_pozos_3w():
    mapeo = mapear_pozos(["WELL-00003", "WELL-00001", "WELL-00002"], [10, 20])
    assert mapeo == {"WELL-00001": 10, "WELL-00002": 20, "WELL-00003": 10}


def test_mapear_pozos_necesita_destinos():
    with pytest.raises(ValueError):
        mapear_pozos(["WELL-00001"], [])

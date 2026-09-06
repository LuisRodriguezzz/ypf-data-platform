"""Armado de la tabla de predicciones que va a `lake.gold.prediccion_produccion_12m`."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from pipelines.ml import datos, predecir

MOMENTO = datetime(2026, 9, 6, 7, 30, 0)


def _no_convencionales(mart: pd.DataFrame) -> pd.DataFrame:
    return datos.preparar(datos.solo_no_convencionales(mart)).reset_index(drop=True)


def test_armar_tabla_tiene_una_fila_por_pozo_no_convencional(mart):
    pozos = _no_convencionales(mart)
    filas = predecir.armar_tabla(pozos, np.arange(len(pozos), dtype=float), "v3", MOMENTO)
    assert list(filas.columns) == list(predecir.COLUMNAS)
    assert filas["idpozo"].tolist() == [1, 2, 3, 4, 5, 6, 8]


def test_el_real_queda_nulo_en_los_pozos_sin_doce_meses(mart):
    pozos = _no_convencionales(mart)
    filas = predecir.armar_tabla(pozos, np.zeros(len(pozos)), "v1", MOMENTO).set_index("idpozo")
    # 4 y 6 todavía no cumplieron el año: no se sabe cuánto van a producir.
    assert filas.loc[[4, 6], "prod_pet_12m_real"].isna().all()
    assert filas.loc[1, "prod_pet_12m_real"] == 30_000.0
    assert filas.loc[5, "prod_pet_12m_real"] == 0.0


def test_la_prediccion_va_para_todos_incluidos_los_incompletos(mart):
    pozos = _no_convencionales(mart)
    filas = predecir.armar_tabla(pozos, np.full(len(pozos), 1234.5), "v1", MOMENTO)
    assert filas["prod_pet_12m_predicho"].notna().all()
    assert (filas["prod_pet_12m_predicho"] == 1234.5).all()


def test_metadatos_de_la_corrida(mart):
    pozos = _no_convencionales(mart)
    filas = predecir.armar_tabla(pozos, np.zeros(len(pozos)), "v7", MOMENTO)
    assert (filas["modelo_version"] == "v7").all()
    assert (filas["predicho_en"] == MOMENTO).all()
    assert (filas["data_origin"] == "derived").all()


def test_los_tipos_entran_en_el_esquema_iceberg(mart):
    pozos = _no_convencionales(mart)
    filas = predecir.armar_tabla(pozos, np.zeros(len(pozos)), "v1", MOMENTO)
    assert filas["idpozo"].dtype == "int64"
    assert filas["prod_pet_12m_predicho"].dtype == "float64"
    assert filas["prod_pet_12m_real"].dtype == "float64"
    # Los nombres del esquema Iceberg y los de la tabla tienen que coincidir uno a uno.
    assert [campo.name for campo in predecir.esquema().fields] == list(predecir.COLUMNAS)

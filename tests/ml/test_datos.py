"""Preparación de features: filtros, topes, derivadas y forma de la matriz."""

from __future__ import annotations

import numpy as np
import pandas as pd

from pipelines.ml import datos


def test_solo_no_convencionales_saca_el_convencional(mart):
    filtrado = datos.solo_no_convencionales(mart)
    assert set(filtrado["idpozo"]) == {1, 2, 3, 4, 5, 6, 8}
    assert (filtrado["tipo_de_recurso"] == "NO CONVENCIONAL").all()


def test_con_objetivo_completo_pide_los_doce_meses(mart):
    completos = datos.con_objetivo_completo(datos.solo_no_convencionales(mart))
    # 4 y 6 tienen historia truncada: su acumulado no es comparable con el de los demás.
    assert set(completos["idpozo"]) == {1, 2, 3, 5, 8}


def test_aplicar_topes_recorta_presion_y_potencia(mart):
    recortado = datos.aplicar_topes(mart)
    assert recortado.loc[recortado["idpozo"] == 2, "presion_maxima_psi"].item() == 20_000.0
    assert (
        recortado.loc[recortado["idpozo"] == 3, "potencia_equipos_fractura_hp"].item() == 100_000.0
    )
    # Los valores por debajo del tope no se tocan.
    assert recortado.loc[recortado["idpozo"] == 1, "presion_maxima_psi"].item() == 10_000.0


def test_aplicar_topes_no_muta_el_original(mart):
    datos.aplicar_topes(mart)
    assert mart.loc[mart["idpozo"] == 2, "presion_maxima_psi"].item() == 209_640.0


def test_agregar_anio_fractura(mart):
    con_anio = datos.agregar_anio_fractura(mart)
    esperado = pd.to_datetime(mart["fecha_inicio_fractura"]).dt.year.astype(float)
    assert con_anio[datos.COLUMNA_ANIO].tolist() == esperado.tolist()


def test_intensidades_son_nulas_en_pozos_verticales(mart):
    con_intensidad = datos.agregar_intensidades(mart)
    vertical = con_intensidad[con_intensidad["longitud_rama_horizontal_m"] == 0.0]
    assert vertical["arena_por_metro"].isna().all()
    assert vertical["etapas_por_metro"].isna().all()
    # El agua por etapa no depende de la rama, así que sí tiene valor.
    assert vertical["agua_por_etapa"].notna().all()


def test_intensidades_de_un_pozo_horizontal(mart):
    pozo = datos.agregar_intensidades(mart).set_index("idpozo").loc[1]
    assert pozo["arena_por_metro"] == 40 * 300.0 / 2500.0
    assert pozo["agua_por_etapa"] == 1200.0
    assert pozo["etapas_por_metro"] == 40 / 2500.0


def test_matriz_features_tiene_las_columnas_esperadas_en_orden(mart):
    matriz = datos.matriz_features(datos.preparar(mart))
    assert list(matriz.columns) == list(datos.COLUMNAS_ENTRADA)
    # Las categóricas van como texto y las demás como float: es lo que espera el pipeline.
    assert all(matriz[columna].dtype == object for columna in datos.COLUMNAS_CATEGORICAS)
    numericas = [c for c in matriz.columns if c not in datos.COLUMNAS_CATEGORICAS]
    assert all(matriz[columna].dtype == float for columna in numericas)


def test_matriz_features_reemplaza_categoricas_nulas(mart):
    mart.loc[0, "sub_tipo_recurso"] = None
    matriz = datos.matriz_features(datos.preparar(mart))
    assert matriz.loc[0, "sub_tipo_recurso"] == datos.SIN_DATO


def test_objetivo_y_vuelta_a_la_escala_original(mart):
    y = datos.objetivo_log(mart)
    assert y.max() == np.log1p(60_000.0)
    vuelta = datos.a_escala_original(y.to_numpy())
    assert np.allclose(vuelta, mart["prod_pet_12m"].to_numpy())


def test_a_escala_original_no_devuelve_negativos():
    assert datos.a_escala_original(np.array([-5.0, 0.0])).tolist() == [0.0, 0.0]

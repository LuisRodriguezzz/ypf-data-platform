"""Validación por grupo: que ningún yacimiento quede a los dos lados del split."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold

from pipelines.ml import datos, entrenar


def _pozos_sinteticos(pozos_por_yacimiento: int = 6, yacimientos: int = 4) -> pd.DataFrame:
    """Pozos de mentira con la señal que el modelo tiene que encontrar: más etapas, más petróleo."""
    generador = np.random.default_rng(0)
    filas = []
    for indice in range(yacimientos):
        for pozo in range(pozos_por_yacimiento):
            etapas = 5 + pozo * 8
            filas.append(
                {
                    "idpozo": indice * 100 + pozo,
                    "areayacimiento": f"YAC-{indice}",
                    "cuenca": "NEUQUINA",
                    "formacion": "vaca muerta",
                    "tipo_terminacion": "Tapón disparo",
                    "sub_tipo_recurso": "SHALE",
                    "profundidad": 3000.0,
                    "fecha_inicio_fractura": pd.Timestamp("2022-01-01"),
                    "longitud_rama_horizontal_m": 2000.0,
                    "cantidad_fracturas": float(etapas),
                    "arena_bombeada_total_tn": etapas * 300.0,
                    "agua_inyectada_m3": etapas * 1200.0,
                    "co2_inyectado_m3": 0.0,
                    "presion_maxima_psi": 10_000.0,
                    "potencia_equipos_fractura_hp": 40_000.0,
                    "duracion_dias": 20.0,
                    "prod_pet_12m": etapas * 800.0 + generador.normal(0, 200),
                }
            )
    return datos.preparar(pd.DataFrame(filas))


def test_group_kfold_no_reparte_un_yacimiento_entre_train_y_test():
    pozos = _pozos_sinteticos()
    x = datos.matriz_features(pozos)
    y = datos.objetivo_log(pozos)
    grupos = pozos[datos.GRUPO]
    for train, test in GroupKFold(n_splits=4).split(x, y, groups=grupos):
        assert not set(grupos.iloc[train]) & set(grupos.iloc[test])


def test_prediccion_fuera_de_muestra_cubre_todos_los_pozos_una_vez():
    pozos = _pozos_sinteticos()
    x = datos.matriz_features(pozos)
    y = datos.objetivo_log(pozos)
    prediccion, por_fold = entrenar.predecir_fuera_de_muestra(
        entrenar.modelo_mediana, x, y, pozos[datos.GRUPO], folds=4
    )
    assert len(prediccion) == len(pozos)
    assert len(por_fold) == 4
    # La mediana del train nunca es 0 acá: si algún pozo quedó sin predecir se nota.
    assert (prediccion > 0).all()


def test_medir_devuelve_las_dos_escalas():
    y = pd.Series(np.log1p([1000.0, 2000.0, 3000.0]))
    metricas = entrenar.medir(y, y.to_numpy())
    assert metricas.r2_log == 1.0
    assert metricas.r2_m3 == 1.0
    assert metricas.mae_m3 == 0.0


def test_el_modelo_le_gana_a_la_mediana_cuando_hay_senal():
    pozos = _pozos_sinteticos(pozos_por_yacimiento=10, yacimientos=5)
    x = datos.matriz_features(pozos)
    y = datos.objetivo_log(pozos)
    grupos = pozos[datos.GRUPO]
    mediana, _ = entrenar.predecir_fuera_de_muestra(entrenar.modelo_mediana, x, y, grupos, 5)
    hgb, _ = entrenar.predecir_fuera_de_muestra(
        lambda: entrenar.modelo_hgb(**entrenar.GRILLA[0]), x, y, grupos, 5
    )
    assert entrenar.medir(y, hgb).r2_log > entrenar.medir(y, mediana).r2_log


def test_tabla_folds_numera_desde_uno():
    y = pd.Series(np.log1p([1000.0, 2000.0]))
    tabla = entrenar.tabla_folds([entrenar.medir(y, y.to_numpy())] * 3)
    assert tabla["fold"].tolist() == [1, 2, 3]
    assert "r2_log" in tabla.columns

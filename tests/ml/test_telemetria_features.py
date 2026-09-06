"""Ventanas deslizantes sobre telemetría: features, etiquetado y split sin fuga.

La serie es sintética y con pendiente conocida a propósito: sobre datos reales no se puede
afirmar cuánto tiene que dar una feature, y estas son las funciones que comparten el
entrenamiento y la inferencia. Si se corren, los dos se rompen juntos y en silencio.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sklearn.model_selection import GroupKFold, KFold

from pipelines.ml import telemetria_features as tf

# La serie: 900 segundos a 1 Hz, normal hasta el 400, transitorio hasta el 700, evento después.
SEGUNDOS = 900
FIN_NORMAL = 400
FIN_TRANSITORIO = 700
PENDIENTE_PDG = 0.5
INICIO = pd.Timestamp("2026-09-06 00:00:00")


def _clase(segundo: int) -> int:
    """Las tres etapas de una instancia de 3W: 0 normal, 102 transitorio, 2 evento."""
    if segundo < FIN_NORMAL:
        return 0
    if segundo < FIN_TRANSITORIO:
        return 102
    return 2


@pytest.fixture
def instancia() -> pd.DataFrame:
    """Una instancia de juguete: `p_pdg` con pendiente conocida y `t_tpt` entero en nulo."""
    segundos = np.arange(SEGUNDOS)
    return pd.DataFrame(
        {
            tf.COLUMNA_TIEMPO: [INICIO + pd.Timedelta(seconds=int(s)) for s in segundos],
            tf.COLUMNA_CLASE: [_clase(int(s)) for s in segundos],
            "p_pdg": 100.0 + PENDIENTE_PDG * segundos,
            "p_tpt": np.full(SEGUNDOS, 7.0),
            "t_tpt": np.full(SEGUNDOS, np.nan),
        }
    )


def _ventana_en(ventanas: pd.DataFrame, offset_s: int) -> pd.Series:
    """La ventana que arranca a `offset_s` segundos del inicio de la instancia."""
    momento = INICIO + pd.Timedelta(seconds=offset_s)
    return ventanas.set_index("ventana_inicio").loc[momento]


def test_pendiente_recupera_la_recta_conocida():
    segundos = np.arange(180.0)
    assert tf.pendiente(segundos, 3.0 + 2.5 * segundos) == pytest.approx(2.5)


def test_pendiente_es_cero_en_una_serie_plana():
    segundos = np.arange(180.0)
    assert tf.pendiente(segundos, np.full(180, 42.0)) == pytest.approx(0.0)


def test_pendiente_sin_variacion_en_el_tiempo_no_existe():
    # Todas las muestras en el mismo instante: no hay recta que ajustar.
    assert np.isnan(tf.pendiente(np.zeros(5), np.arange(5.0)))


def test_resumen_sensor_sobre_una_rampa():
    segundos = np.arange(180.0)
    resumen = tf.resumen_sensor(segundos, 100.0 + PENDIENTE_PDG * segundos)
    assert resumen["minimo"] == pytest.approx(100.0)
    assert resumen["maximo"] == pytest.approx(100.0 + PENDIENTE_PDG * 179)
    assert resumen["media"] == pytest.approx(100.0 + PENDIENTE_PDG * 89.5)
    assert resumen["delta"] == pytest.approx(PENDIENTE_PDG * 179)
    assert resumen["pendiente"] == pytest.approx(PENDIENTE_PDG)


def test_resumen_sensor_de_un_sensor_entero_en_nulo():
    # Es el caso normal en 3W: los archivos viejos traen 23 de 27 sensores vacíos.
    resumen = tf.resumen_sensor(np.arange(180.0), np.full(180, np.nan))
    assert set(resumen) == set(tf.ESTADISTICOS)
    assert all(np.isnan(valor) for valor in resumen.values())


def test_resumen_sensor_ignora_los_nulos_sueltos():
    valores = np.array([1.0, np.nan, 3.0, np.nan, 5.0])
    resumen = tf.resumen_sensor(np.arange(5.0), valores)
    assert resumen["media"] == pytest.approx(3.0)
    assert resumen["delta"] == pytest.approx(4.0)


@pytest.mark.parametrize(
    ("codigo", "esperada"),
    [(0, tf.NORMAL), (2, tf.EVENTO), (7, tf.EVENTO), (102, tf.TRANSITORIO), (107, tf.TRANSITORIO)],
)
def test_etiqueta_de_clase(codigo, esperada):
    assert tf.etiqueta_de_clase(codigo) == esperada


def test_etiqueta_de_clase_sin_dato():
    # `class` viene en nulo en el arranque de las instancias: esas filas no se pueden etiquetar.
    assert tf.etiqueta_de_clase(float("nan")) is None
    assert tf.etiqueta_de_clase(None) is None


def test_etiqueta_de_ventana_toma_la_mas_severa():
    assert tf.etiqueta_de_ventana(np.array([0.0, 0.0])) == tf.NORMAL
    assert tf.etiqueta_de_ventana(np.array([0.0, 102.0])) == tf.TRANSITORIO
    assert tf.etiqueta_de_ventana(np.array([0.0, 102.0, 2.0])) == tf.EVENTO


def test_etiqueta_de_ventana_sin_ninguna_fila_etiquetada():
    assert tf.etiqueta_de_ventana(np.array([np.nan, np.nan])) is None


def test_construir_ventanas_cubre_la_instancia_con_el_paso_pedido(instancia):
    ventanas = tf.construir_ventanas(instancia, "test", max_ventanas=None)
    # Última muestra en el segundo 899; la última ventana entera arranca en el 705.
    assert len(ventanas) == 48
    assert ventanas["ventana_inicio"].iloc[0] == INICIO
    assert ventanas["ventana_fin"].iloc[0] == INICIO + pd.Timedelta(seconds=tf.VENTANA_S)
    diferencias = ventanas["ventana_inicio"].diff().dropna().unique()
    assert list(diferencias) == [pd.Timedelta(seconds=tf.PASO_S)]


def test_las_features_de_una_ventana_son_las_de_la_rampa(instancia):
    ventanas = tf.construir_ventanas(instancia, "test", max_ventanas=None)
    primera = _ventana_en(ventanas, 0)
    assert primera["p_pdg_pendiente"] == pytest.approx(PENDIENTE_PDG)
    assert primera["p_pdg_minimo"] == pytest.approx(100.0)
    assert primera["p_pdg_delta"] == pytest.approx(PENDIENTE_PDG * (tf.VENTANA_S - 1))
    # Un sensor plano tiene desvío y pendiente cero, que no es lo mismo que no tener dato.
    assert primera["p_tpt_desvio"] == pytest.approx(0.0)
    assert primera["p_tpt_pendiente"] == pytest.approx(0.0)


def test_un_sensor_nulo_o_ausente_da_features_nulas(instancia):
    ventanas = tf.construir_ventanas(instancia, "test", max_ventanas=None)
    # `t_tpt` está entero en nulo y `p_mon_ckp` ni siquiera viene en la instancia.
    assert ventanas["t_tpt_media"].isna().all()
    assert ventanas["p_mon_ckp_media"].isna().all()
    # El resto de las features sí tiene valor: un sensor faltante no invalida la ventana.
    assert ventanas["p_pdg_media"].notna().all()


def test_las_ventanas_se_etiquetan_por_la_etapa_que_tocan(instancia):
    ventanas = tf.construir_ventanas(instancia, "test", max_ventanas=None)
    # [210, 390) cae entera en el tramo normal; [225, 405) ya toca el transitorio.
    assert _ventana_en(ventanas, 210)["etiqueta"] == tf.NORMAL
    assert _ventana_en(ventanas, 225)["etiqueta"] == tf.TRANSITORIO
    # [510, 690) sigue en el transitorio; [525, 705) toca los primeros segundos del evento.
    assert _ventana_en(ventanas, 510)["etiqueta"] == tf.TRANSITORIO
    assert _ventana_en(ventanas, 525)["etiqueta"] == tf.EVENTO


def test_las_ventanas_sin_class_quedan_sin_etiqueta(instancia):
    sin_clase = instancia.drop(columns=[tf.COLUMNA_CLASE])
    ventanas = tf.construir_ventanas(sin_clase, "test", max_ventanas=None)
    # Es el caso de la inferencia: la etiqueta es justamente lo que hay que predecir.
    assert ventanas["etiqueta"].isna().all()
    assert tf.solo_etiquetadas(ventanas).empty


def test_una_instancia_mas_corta_que_la_ventana_no_aporta_nada(instancia):
    corta = instancia.head(tf.VENTANA_S - 1)
    assert tf.construir_ventanas(corta, "test").empty


def test_con_datos_descarta_las_ventanas_sin_ningun_sensor(instancia):
    sin_sensores = instancia.drop(columns=["p_pdg", "p_tpt", "t_tpt"])
    ventanas = tf.construir_ventanas(sin_sensores, "test", max_ventanas=None)
    assert len(ventanas) == 48
    assert tf.con_datos(ventanas).empty


def test_paso_adaptativo_ensancha_solo_las_instancias_largas():
    # Una instancia de 3 horas entra holgada en el tope y conserva el paso de 15 s.
    assert tf.paso_adaptativo(3 * 3600, 15, 1500) == 15
    # Una de 100 horas aportaría 24.000 ventanas: el paso se ensancha para que entren 1.500.
    assert tf.paso_adaptativo(100 * 3600, 15, 1500) == 240
    assert tf.paso_adaptativo(100 * 3600, 15, None) == 15


def test_el_tope_de_ventanas_por_instancia_se_respeta(instancia):
    ventanas = tf.construir_ventanas(instancia, "test", max_ventanas=10)
    assert len(ventanas) <= 10
    # Se conserva la instancia entera, no su primer tramo: la última ventana sigue en el evento.
    assert ventanas["etiqueta"].iloc[-1] == tf.EVENTO


def test_inicio_de_etiqueta(instancia):
    assert tf.inicio_de_etiqueta(instancia, tf.NORMAL) == INICIO
    assert tf.inicio_de_etiqueta(instancia, tf.TRANSITORIO) == INICIO + pd.Timedelta(
        seconds=FIN_NORMAL
    )
    assert tf.inicio_de_etiqueta(instancia, tf.EVENTO) == INICIO + pd.Timedelta(
        seconds=FIN_TRANSITORIO
    )


def test_inicio_de_etiqueta_cuando_el_evento_nunca_ocurre(instancia):
    # 31 de las 36 instancias de clase 7 en landing terminan en el transitorio, sin evento.
    solo_transitorio = instancia[instancia[tf.COLUMNA_CLASE] != 2]
    assert tf.inicio_de_etiqueta(solo_transitorio, tf.EVENTO) is None


def test_normalizar_3w_deja_los_nombres_de_bronze():
    crudo = pd.DataFrame({"timestamp": [INICIO], "P-PDG": [1.0], "P-MON-CKP": [2.0], "class": [0]})
    normalizado = tf.normalizar_3w(crudo)
    assert list(normalizado.columns) == [tf.COLUMNA_TIEMPO, "p_pdg", "p_mon_ckp", tf.COLUMNA_CLASE]


def _dataset_de_tres_instancias(instancia: pd.DataFrame) -> pd.DataFrame:
    """Las mismas ventanas repetidas en tres instancias de dos pozos distintos."""
    partes = []
    for numero, pozo in enumerate(("WELL-A", "WELL-A", "WELL-B")):
        ventanas = tf.construir_ventanas(instancia, f"inst-{numero}", max_ventanas=None)
        partes.append(ventanas.assign(well_3w=pozo))
    return pd.concat(partes, ignore_index=True)


def test_el_split_por_instancia_no_parte_una_instancia(instancia):
    ventanas = _dataset_de_tres_instancias(instancia)
    grupos = ventanas["instancia_id"]
    for train, test in GroupKFold(n_splits=3).split(ventanas, groups=grupos):
        assert not set(grupos.iloc[train]) & set(grupos.iloc[test])


def test_el_split_por_pozo_tampoco_parte_un_pozo(instancia):
    ventanas = _dataset_de_tres_instancias(instancia)
    grupos = ventanas["well_3w"]
    for train, test in GroupKFold(n_splits=2).split(ventanas, groups=grupos):
        assert not set(grupos.iloc[train]) & set(grupos.iloc[test])


def test_el_split_aleatorio_si_reparte_la_misma_instancia(instancia):
    """La fuga que el `GroupKFold` evita, hecha explícita.

    Dos ventanas consecutivas comparten 165 de sus 180 segundos: con un split aleatorio casi
    toda ventana de test tiene su gemela en train y el modelo aprueba por haberla visto.
    """
    ventanas = _dataset_de_tres_instancias(instancia)
    grupos = ventanas["instancia_id"]
    train, test = next(iter(KFold(n_splits=3, shuffle=True, random_state=0).split(ventanas)))
    assert set(grupos.iloc[train]) & set(grupos.iloc[test])

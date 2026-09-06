"""Ventanas deslizantes sobre la telemetría a 1 Hz de una instancia de 3W.

Todo lo que decide qué ve el clasificador de eventos vive acá, en funciones puras sobre
DataFrames. El entrenamiento (que lee los Parquet de landing) y la inferencia batch (que lee
`lake.bronze.telemetria_pozo`) tienen que armar la ventana exactamente igual, y la única forma
barata de garantizarlo es que llamen a las mismas funciones. Es el mismo criterio de
`pipelines/ml/datos.py` para el modelo de completación.

Una **instancia** es un archivo de 3W: un pozo de Petrobras registrado de corrido durante horas
o días, con la etiqueta `class` de un especialista en cada segundo. Una **ventana** son 180
segundos consecutivos de esa instancia, y es la unidad que el modelo clasifica.

No se importa nada de MLflow, Iceberg ni boto3 a propósito: esto se testea sin infraestructura.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from pipelines.streaming.eventos import nombre_columna

# Nombres tal como llegan de bronze; `normalizar_3w` lleva los del Parquet crudo a estos.
COLUMNA_TIEMPO = "event_time"
COLUMNA_CLASE = "class"

# Los cinco sensores que entran al modelo, elegidos por cobertura medida sobre las 88
# instancias de landing (archivos con al menos un valor): T-TPT 84, P-TPT 80, P-ANULAR 76,
# P-PDG 75, P-MON-CKP 66. Los cuatro primeros de `streaming.eventos.SENSORES_CLAVE` son los
# que mueve un cierre espurio de DHSV (presión y temperatura de fondo); `p_mon_ckp` es la
# presión aguas arriba del choke de producción, que es donde se ve el scaling de la clase 7.
# `p_jus_ckp` (aguas abajo del choke) sería el par natural de `p_mon_ckp`, pero está poblado
# en 1 de las 88 instancias: no se puede usar.
SENSORES = ("p_pdg", "p_tpt", "t_tpt", "p_mon_ckp", "p_anular")

# Seis estadísticos por sensor. Los cuatro primeros describen el nivel de la ventana; los dos
# últimos, cómo se movió dentro de ella, que es lo único que separa una degradación lenta
# (scaling) de un valor alto y estable.
ESTADISTICOS = ("media", "desvio", "minimo", "maximo", "pendiente", "delta")

# 180 s de ventana con paso de 15 s es lo que sugiere el toolkit de 3W para la clase 2: el
# cierre espurio del DHSV se ve en las presiones en segundos, pero hace falta contexto para
# distinguirlo del ruido. El paso de 15 s deja 12 ventanas superpuestas por cada punto.
VENTANA_S = 180
PASO_S = 15

# Ninguna instancia aporta más ventanas que esto. Las instancias de clase 7 duran hasta 213
# horas: con paso de 15 s una sola aportaría 51.000 ventanas y decidiría el dataset entero.
# Se ensancha el paso en vez de recortar la instancia, así se conserva el tramo normal, el
# transitorio y el evento (a menor resolución temporal, que es lo que se paga).
MAX_VENTANAS_POR_INSTANCIA = 1500

NORMAL = "normal"
TRANSITORIO = "transitorio"
EVENTO = "evento"
ETIQUETAS = (NORMAL, TRANSITORIO, EVENTO)

# De menos a más severa: la etiqueta de una ventana es la más severa que toca.
SEVERIDAD = {NORMAL: 0, TRANSITORIO: 1, EVENTO: 2}

# En 3W la etiqueta del transitorio previo al evento N es 100+N (ver docs/fuentes/telemetria_3w.md).
DESPLAZAMIENTO_TRANSITORIO = 100

COLUMNAS_META = ("instancia_id", "ventana_inicio", "ventana_fin", "etiqueta")


def nombres_features() -> list[str]:
    """Las 30 columnas de entrada del modelo, en orden fijo (sensor por sensor)."""
    return [f"{sensor}_{estadistico}" for sensor in SENSORES for estadistico in ESTADISTICOS]


def normalizar_3w(telemetria: pd.DataFrame) -> pd.DataFrame:
    """Nombres del Parquet crudo a los de bronze: `P-PDG` -> `p_pdg`, `timestamp` -> `event_time`.

    Así el entrenamiento, que lee landing, y la inferencia, que lee bronze, le pasan a
    `construir_ventanas` exactamente las mismas columnas.
    """
    renombres = {columna: nombre_columna(columna) for columna in telemetria.columns}
    renombres["timestamp"] = COLUMNA_TIEMPO
    return telemetria.rename(columns=renombres)


def etiqueta_de_clase(codigo: float) -> str | None:
    """`0` -> normal, `100+N` -> transitorio, `N` -> evento. `None` si la fila no tiene etiqueta.

    Las instancias de 3W arrancan con `class` en nulo (la primera hora del archivo de 2013 que
    documenta `docs/fuentes/telemetria_3w.md`): esas filas no se pueden etiquetar y se descartan.
    """
    if codigo is None or (isinstance(codigo, float) and math.isnan(codigo)):
        return None
    entero = int(codigo)
    if entero == 0:
        return NORMAL
    if entero >= DESPLAZAMIENTO_TRANSITORIO:
        return TRANSITORIO
    return EVENTO


def etiquetas_de_serie(codigos: pd.Series) -> pd.Series:
    """La etiqueta de cada fila, mapeando solo los códigos distintos.

    Una instancia tiene tres o cuatro códigos en cientos de miles de filas: mapear los únicos
    y no fila por fila es la diferencia entre un segundo y un minuto sobre el corpus entero.
    """
    unicos = codigos.dropna().unique()
    return codigos.map({codigo: etiqueta_de_clase(codigo) for codigo in unicos})


def etiqueta_de_ventana(codigos: np.ndarray) -> str | None:
    """La etiqueta más severa que toca la ventana: evento > transitorio > normal.

    Las instancias de 3W son monótonas (normal, después transitorio, después evento), así que
    esto coincide con el estado en el que la ventana termina. Se elige por severidad y no por
    el último valor porque no depende de ese supuesto: una ventana que ya toca el evento es
    una ventana de evento aunque el archivo vuelva a normal más adelante.
    """
    presentes = {etiqueta_de_clase(codigo) for codigo in np.unique(codigos[~np.isnan(codigos)])}
    presentes.discard(None)
    if not presentes:
        return None
    return max(presentes, key=lambda etiqueta: SEVERIDAD[etiqueta])


def pendiente(segundos: np.ndarray, valores: np.ndarray) -> float:
    """Pendiente de la recta de mínimos cuadrados, en unidades del sensor por segundo.

    Es la feature que distingue una degradación lenta —el scaling en el choke, clase 7— de un
    valor alto pero estable: el nivel lo dan media, mínimo y máximo; la tendencia, solo esta.
    """
    centrado = segundos - segundos.mean()
    varianza = float((centrado**2).sum())
    if varianza == 0.0:
        # Todas las muestras en el mismo instante: no hay recta que ajustar.
        return float("nan")
    return float((centrado * (valores - valores.mean())).sum() / varianza)


def resumen_sensor(segundos: np.ndarray, valores: np.ndarray) -> dict[str, float]:
    """Los seis estadísticos de un sensor en una ventana; todo `NaN` si no hay dos muestras.

    Un sensor entero en nulo es lo normal en 3W: los archivos viejos traen 23 de 27 sensores
    vacíos. Devolver `NaN` y dejar que el HistGradientBoosting lo trate como faltante es más
    honesto que imputar un cero que el instrumento nunca midió.
    """
    validos = ~np.isnan(valores)
    if int(validos.sum()) < 2:
        return dict.fromkeys(ESTADISTICOS, float("nan"))
    x, y = segundos[validos], valores[validos]
    return {
        "media": float(y.mean()),
        "desvio": float(y.std()),
        "minimo": float(y.min()),
        "maximo": float(y.max()),
        "pendiente": pendiente(x, y),
        "delta": float(y[-1] - y[0]),
    }


def paso_adaptativo(duracion_s: float, paso_s: int, max_ventanas: int | None) -> int:
    """Ensancha el paso si la instancia es tan larga que aportaría más ventanas que el tope."""
    if not max_ventanas or duracion_s <= 0:
        return paso_s
    return max(paso_s, math.ceil(duracion_s / max_ventanas))


def offsets_de_ventanas(duracion_s: float, ventana_s: int, paso_s: int) -> np.ndarray:
    """Segundos desde el inicio en los que arranca cada ventana completa.

    Solo ventanas enteras: una instancia más corta que 180 s no aporta ninguna.
    """
    ultimo = duracion_s - ventana_s
    if ultimo < 0:
        return np.empty(0)
    offsets = np.arange(0.0, ultimo + paso_s, float(paso_s))
    return offsets[offsets <= ultimo]


def _columna_o_nulos(telemetria: pd.DataFrame, nombre: str) -> np.ndarray:
    """La columna como float64, o una de nulos si la instancia no la trae."""
    if nombre not in telemetria.columns:
        return np.full(len(telemetria), np.nan)
    return telemetria[nombre].astype("float64").to_numpy()


def construir_ventanas(
    telemetria: pd.DataFrame,
    instancia_id: str,
    ventana_s: int = VENTANA_S,
    paso_s: int = PASO_S,
    max_ventanas: int | None = MAX_VENTANAS_POR_INSTANCIA,
) -> pd.DataFrame:
    """Una fila por ventana de 180 s: metadatos, etiqueta y los 30 estadísticos.

    `telemetria` es UNA instancia (un archivo de 3W o un pozo de bronze) con `event_time`, los
    sensores y, si existe, `class`. Sin `class` la etiqueta queda nula: es el caso de la
    inferencia, donde la etiqueta es justamente lo que hay que predecir.

    Los cortes se hacen por tiempo y no por posición: el 1 Hz de 3W está medido pero un hueco
    en bronze (un corte del enlace) no debe correr todas las ventanas siguientes.
    """
    orden = telemetria.sort_values(COLUMNA_TIEMPO)
    if orden.empty:
        return pd.DataFrame(columns=[*COLUMNAS_META, *nombres_features()])

    tiempos = pd.to_datetime(orden[COLUMNA_TIEMPO])
    inicio = tiempos.iloc[0]
    segundos = (tiempos - inicio).dt.total_seconds().to_numpy()
    codigos = _columna_o_nulos(orden, COLUMNA_CLASE)
    valores = {sensor: _columna_o_nulos(orden, sensor) for sensor in SENSORES}

    paso = paso_adaptativo(float(segundos[-1]), paso_s, max_ventanas)
    filas = []
    for offset in offsets_de_ventanas(float(segundos[-1]), ventana_s, paso):
        desde = int(np.searchsorted(segundos, offset, "left"))
        hasta = int(np.searchsorted(segundos, offset + ventana_s, "left"))
        fila = {
            "instancia_id": instancia_id,
            "ventana_inicio": inicio + pd.Timedelta(seconds=offset),
            "ventana_fin": inicio + pd.Timedelta(seconds=offset + ventana_s),
            "etiqueta": etiqueta_de_ventana(codigos[desde:hasta]),
        }
        for sensor, serie in valores.items():
            resumen = resumen_sensor(segundos[desde:hasta], serie[desde:hasta])
            fila.update({f"{sensor}_{clave}": valor for clave, valor in resumen.items()})
        filas.append(fila)
    return pd.DataFrame(filas, columns=[*COLUMNAS_META, *nombres_features()])


def solo_etiquetadas(ventanas: pd.DataFrame) -> pd.DataFrame:
    """Las ventanas que caen sobre filas con `class`: las únicas que se pueden entrenar."""
    return ventanas[ventanas["etiqueta"].notna()].reset_index(drop=True)


def con_datos(ventanas: pd.DataFrame) -> pd.DataFrame:
    """Descarta las ventanas en las que ninguno de los cinco sensores midió nada.

    Con las 30 features en nulo el modelo predice la clase mayoritaria y nada más: es ruido
    tanto para entrenar como para alertar.
    """
    hay_dato = ventanas[nombres_features()].notna().any(axis=1)
    return ventanas[hay_dato].reset_index(drop=True)


def inicio_de_etiqueta(telemetria: pd.DataFrame, etiqueta: str) -> pd.Timestamp | None:
    """Primer instante de la instancia con esa etiqueta, o `None` si nunca aparece.

    Es el punto contra el que se mide el tiempo de anticipación: cuánto antes del primer
    segundo de evento el modelo levantó la mano.
    """
    if COLUMNA_CLASE not in telemetria.columns:
        return None
    orden = telemetria.sort_values(COLUMNA_TIEMPO)
    coincide = etiquetas_de_serie(orden[COLUMNA_CLASE].astype("float64")) == etiqueta
    if not coincide.any():
        return None
    return pd.to_datetime(orden.loc[coincide, COLUMNA_TIEMPO].iloc[0])

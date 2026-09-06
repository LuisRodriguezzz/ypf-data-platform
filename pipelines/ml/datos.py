"""Carga del mart de completación y preparación de features.

Todo lo que decide qué entra al modelo vive acá, en funciones puras sobre DataFrames: el
entrenamiento y la inferencia batch tienen que preparar los datos exactamente igual, y la
única forma barata de garantizarlo es que llamen a las mismas funciones.

La tabla de origen es `lake.gold.mart_pozo_completacion_produccion`: un pozo fracturado por
fila, con el diseño de su completación de un lado y lo que produjo del otro.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyiceberg.catalog.rest import RestCatalog

from pipelines.spark_jobs.config import LakehouseConfig

NAMESPACE = "gold"
TABLA_MART = "mart_pozo_completacion_produccion"

# El objetivo es el acumulado de petróleo de los primeros 12 meses. Se modela en log1p porque
# la distribución va de 0 a 155.000 m3: en escala original el error de los pozos grandes se
# come al de todos los demás.
OBJETIVO = "prod_pet_12m"

# El split de validación agrupa por yacimiento (ver entrenar.py y ADR 0012).
GRUPO = "areayacimiento"

# Solo el no convencional: en el convencional la fractura es una intervención puntual sobre
# un pozo que ya producía, así que la relación entre completación y producción es otra.
TIPO_RECURSO = "NO CONVENCIONAL"

# Un pozo con menos de 12 meses declarados tiene el acumulado truncado: entrenar con él sería
# enseñarle al modelo que ese diseño produce poco cuando lo que pasa es que le falta historia.
MESES_COMPLETOS = 12

# Columnas del mart que entran tal cual como features numéricas.
COLUMNAS_NUMERICAS = (
    "longitud_rama_horizontal_m",
    "cantidad_fracturas",
    "arena_bombeada_total_tn",
    "agua_inyectada_m3",
    "co2_inyectado_m3",
    "presion_maxima_psi",
    "potencia_equipos_fractura_hp",
    "duracion_dias",
    "profundidad",
)

# Intensidades de la completación: son las tres medidas con las que la industria compara
# diseños entre pozos de largo distinto. Suben el R² de 0,30 a 0,34 fuera de muestra.
COLUMNAS_DERIVADAS = ("arena_por_metro", "agua_por_etapa", "etapas_por_metro")

# Año de la fractura: la tecnología de completación cambió mucho entre 2011 y 2025 y el
# modelo necesita poder distinguir épocas.
COLUMNA_ANIO = "anio_fractura"

COLUMNAS_CATEGORICAS = ("cuenca", "formacion", "tipo_terminacion", "sub_tipo_recurso")

# Sin dato explícito en vez de nulo: el codificador ordinal necesita un string.
SIN_DATO = "(sin dato)"

# Topes físicos documentados en docs/fuentes/fractura.md. El contrato de silver ya manda a
# cuarentena lo que los supera, pero el mart puede traer valores de antes de esa regla: se
# recortan en vez de descartar el pozo, que tiene el resto de las columnas sanas.
TOPES = {
    "presion_maxima_psi": 20_000.0,
    "potencia_equipos_fractura_hp": 100_000.0,
}

COLUMNAS_ENTRADA = (*COLUMNAS_NUMERICAS, *COLUMNAS_DERIVADAS, COLUMNA_ANIO, *COLUMNAS_CATEGORICAS)


def abrir_catalogo(config: LakehouseConfig) -> RestCatalog:
    """Catálogo Iceberg REST apuntando a MinIO, igual que scripts/check_lake.py."""
    return RestCatalog(
        "lake",
        **{
            "uri": config.iceberg_catalog_uri,
            "warehouse": config.iceberg_warehouse,
            "s3.endpoint": config.s3_endpoint_url,
            "s3.access-key-id": config.s3_access_key_id,
            "s3.secret-access-key": config.s3_secret_access_key,
            "s3.region": config.s3_region,
        },
    )


def leer_mart(catalogo: RestCatalog) -> pd.DataFrame:
    """El mart entero en memoria: son 4.635 filas y 30 columnas, no hace falta filtrar antes."""
    return catalogo.load_table(f"{NAMESPACE}.{TABLA_MART}").scan().to_pandas()


def solo_no_convencionales(mart: pd.DataFrame) -> pd.DataFrame:
    """Los pozos sobre los que el modelo tiene algo que decir."""
    return mart[mart["tipo_de_recurso"] == TIPO_RECURSO].copy()


def con_objetivo_completo(pozos: pd.DataFrame) -> pd.DataFrame:
    """Los que declararon los 12 meses: son los únicos con un target comparable."""
    return pozos[pozos["meses_con_declaracion"] == MESES_COMPLETOS].copy()


def aplicar_topes(pozos: pd.DataFrame) -> pd.DataFrame:
    """Recorta presión y potencia en sus máximos físicos (docs/fuentes/fractura.md)."""
    recortado = pozos.copy()
    for columna, tope in TOPES.items():
        recortado[columna] = recortado[columna].astype(float).clip(upper=tope)
    return recortado


def agregar_anio_fractura(pozos: pd.DataFrame) -> pd.DataFrame:
    """Año de `fecha_inicio_fractura` como número, para que el modelo lo pueda partir."""
    con_anio = pozos.copy()
    con_anio[COLUMNA_ANIO] = pd.to_datetime(con_anio["fecha_inicio_fractura"]).dt.year.astype(float)
    return con_anio


def agregar_intensidades(pozos: pd.DataFrame) -> pd.DataFrame:
    """Arena por metro, agua por etapa y etapas por metro.

    La rama horizontal vale 0 en los pozos verticales: dividir por ella da infinito, así que
    el 0 pasa a nulo y el HistGradientBoosting lo trata como categoría faltante (sabe hacerlo).
    """
    con_intensidad = pozos.copy()
    rama = con_intensidad["longitud_rama_horizontal_m"].astype(float).replace(0.0, np.nan)
    etapas = con_intensidad["cantidad_fracturas"].astype(float).replace(0.0, np.nan)
    con_intensidad["arena_por_metro"] = con_intensidad["arena_bombeada_total_tn"] / rama
    con_intensidad["agua_por_etapa"] = con_intensidad["agua_inyectada_m3"] / etapas
    con_intensidad["etapas_por_metro"] = con_intensidad["cantidad_fracturas"] / rama
    return con_intensidad


def preparar(pozos: pd.DataFrame) -> pd.DataFrame:
    """Las tres transformaciones que van sí o sí antes de armar la matriz."""
    return agregar_intensidades(agregar_anio_fractura(aplicar_topes(pozos)))


def matriz_features(pozos: pd.DataFrame) -> pd.DataFrame:
    """Las columnas de entrada del modelo, en orden fijo y con los tipos que espera.

    El orden importa: el `ColumnTransformer` del pipeline referencia las categóricas por
    nombre, pero el `categorical_features` del HistGradientBoosting va por posición.
    """
    numericas = [*COLUMNAS_NUMERICAS, *COLUMNAS_DERIVADAS, COLUMNA_ANIO]
    matriz = pozos[numericas].astype(float).copy()
    for columna in COLUMNAS_CATEGORICAS:
        matriz[columna] = pozos[columna].fillna(SIN_DATO).astype(str)
    return matriz


def objetivo_log(pozos: pd.DataFrame) -> pd.Series:
    """El target en la escala en la que se entrena."""
    return np.log1p(pozos[OBJETIVO].astype(float))


# Techo de cordura para volver de log a m3. El pozo más productivo del dataset acumuló 155.000
# m3 en 12 meses; 10 millones es dos órdenes de magnitud arriba de cualquier valor posible. Sin
# el techo, un modelo que se desmadre en log (la regresión lineal lo hace) devuelve un expm1
# infinito y contamina cualquier métrica en escala original.
TECHO_M3 = 10_000_000.0


def a_escala_original(prediccion_log: np.ndarray) -> np.ndarray:
    """Vuelta de log1p a m3, acotada: un pozo no produce ni menos que nada ni un absurdo."""
    acotado = np.clip(prediccion_log, 0.0, np.log1p(TECHO_M3))
    return np.expm1(acotado)

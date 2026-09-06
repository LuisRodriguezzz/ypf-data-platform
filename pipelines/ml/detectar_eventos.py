"""Inferencia batch: busca eventos de pozo en la telemetría reciente de bronze.

Arma las mismas ventanas de 180 s que el entrenamiento —con las mismas funciones de
`telemetria_features`— sobre las últimas N horas de `event_time` de
`lake.bronze.telemetria_pozo`, las clasifica con el `champion` del registry y deja en
`lake.gold.alerta_evento_pozo` una fila por ventana que **no** salió `normal`. Las ventanas
normales no se escriben: la tabla es de alertas y no de scoring, y quien quiera el estado
completo de un pozo lo tiene en `lake.silver.telemetria_pozo_1min`.

La tabla se reemplaza entera en cada corrida, como `prediccion_produccion_12m`: es una foto de
la última ventana de N horas con el modelo vigente, y volver a correr el DAG del mismo día da
exactamente el mismo resultado.

**Por qué esto es batch y no streaming.** La inferencia en línea de verdad sería un
`foreachBatch` dentro de `pipelines/streaming/consume_telemetria.py`: por cada micro-lote,
mantener por pozo los últimos 180 s de lecturas en estado (`flatMapGroupsWithState`), armar la
ventana y clasificarla ahí mismo, con lo que la alerta saldría a los segundos en vez de a las
horas. No se implementa ahora por dos razones concretas: el modelo vive en el registry de
MLflow y cargarlo dentro de un ejecutor de Spark obliga a resolver la distribución del
artefacto y su versión en cada worker; y el consumidor tiene su propio checkpoint y su propio
watermark, así que meterle un modelo adentro mezcla dos ciclos de vida —el del pipeline de
datos y el del modelo— en un solo proceso que hay que redeployar cada vez que cambia
cualquiera de los dos. El batch diario da la misma respuesta con una pieza menos que operar.
Cuando el caso de uso pida segundos y no horas, lo que cambia es el motor, no las features:
`construir_ventanas` es la misma función.

Uso: `uv run python -m pipelines.ml.detectar_eventos --horas 24`
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta

import mlflow
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from pipelines.ml import datos
from pipelines.ml import registro_eventos as registro
from pipelines.ml import telemetria_features as tf
from pipelines.spark_jobs.config import load_config

logger = logging.getLogger("ml.detectar_eventos")

NAMESPACE = "gold"
TABLA = "alerta_evento_pozo"
ORIGEN = "bronze.telemetria_pozo"

# La tabla es una derivación del bronze del streaming, no un dato declarado por nadie.
DATA_ORIGIN = "derived"

COLUMNAS = (
    "idpozo",
    "ventana_inicio",
    "ventana_fin",
    "clase_predicha",
    "probabilidad",
    "modelo_version",
    "detectado_en",
    "data_origin",
)

# Columnas que se traen de bronze: las llaves, el tiempo y los cinco sensores del modelo.
COLUMNAS_BRONZE = ("well_3w", "idpozo", tf.COLUMNA_TIEMPO, *tf.SENSORES)


def esquema() -> Schema:
    """Esquema de la tabla de alertas.

    Los tres tiempos van con zona porque `event_time` de bronze la tiene: una alerta sobre
    telemetría de campo sin zona horaria es una alerta que no se puede correlacionar con nada.
    """
    return Schema(
        NestedField(1, "idpozo", LongType(), required=False),
        NestedField(2, "ventana_inicio", TimestamptzType(), required=False),
        NestedField(3, "ventana_fin", TimestamptzType(), required=False),
        NestedField(4, "clase_predicha", StringType(), required=False),
        NestedField(5, "probabilidad", DoubleType(), required=False),
        NestedField(6, "modelo_version", StringType(), required=False),
        NestedField(7, "detectado_en", TimestamptzType(), required=False),
        NestedField(8, "data_origin", StringType(), required=False),
    )


def ultimo_event_time(tabla: Table) -> datetime | None:
    """El `event_time` más nuevo de bronze.

    La ventana de N horas se cuenta desde acá y no desde el reloj: el replay rebasea el tiempo
    de evento (ADR 0011), así que "las últimas 24 horas" son 24 horas de telemetría, no de
    calendario. Se lee una sola columna, que Iceberg resuelve sin tocar el resto del Parquet.
    """
    columna = tabla.scan(selected_fields=(tf.COLUMNA_TIEMPO,)).to_arrow().column(0)
    if len(columna) == 0:
        return None
    return pd.Timestamp(columna.to_pandas().max()).to_pydatetime()


def leer_telemetria(tabla: Table, desde: datetime) -> pd.DataFrame:
    """Las lecturas de bronze desde ese instante, con las columnas que usa el modelo."""
    barrido = tabla.scan(
        row_filter=GreaterThanOrEqual(tf.COLUMNA_TIEMPO, desde.isoformat()),
        selected_fields=COLUMNAS_BRONZE,
    )
    return barrido.to_pandas()


def ventanas_por_pozo(telemetria: pd.DataFrame) -> pd.DataFrame:
    """Ventanas de 180 s de cada pozo, sin tope: en inferencia se quiere toda la resolución.

    El tope de `construir_ventanas` existe para que una instancia de días no domine el
    entrenamiento; acá cada pozo aporta unas horas y ensanchar el paso solo retrasaría la
    alerta.
    """
    partes = []
    for (well, idpozo), grupo in telemetria.groupby(["well_3w", "idpozo"], dropna=False):
        ventanas = tf.construir_ventanas(grupo, instancia_id=str(well), max_ventanas=None)
        partes.append(ventanas.assign(idpozo=idpozo))
    if not partes:
        return pd.DataFrame(columns=[*tf.COLUMNAS_META, "idpozo", *tf.nombres_features()])
    return tf.con_datos(pd.concat(partes, ignore_index=True))


def armar_alertas(
    ventanas: pd.DataFrame, clases, probabilidades, version: str, momento: datetime
) -> pd.DataFrame:
    """Una fila por ventana clasificada como transitorio o evento."""
    todas = pd.DataFrame(
        {
            "idpozo": ventanas["idpozo"].astype("int64").to_numpy(),
            "ventana_inicio": ventanas["ventana_inicio"].to_numpy(),
            "ventana_fin": ventanas["ventana_fin"].to_numpy(),
            "clase_predicha": clases,
            "probabilidad": probabilidades.astype("float64"),
            "modelo_version": version,
            "detectado_en": momento,
            "data_origin": DATA_ORIGIN,
        },
        columns=list(COLUMNAS),
    )
    return todas[todas["clase_predicha"] != tf.NORMAL].reset_index(drop=True)


def asegurar_tabla(catalogo: RestCatalog, identificador: str) -> Table:
    """Crea la tabla la primera vez; después la abre. Sin particiones: son decenas de filas."""
    catalogo.create_namespace_if_not_exists(NAMESPACE)
    return catalogo.create_table_if_not_exists(identificador, schema=esquema())


def escribir(tabla: Table, filas: pd.DataFrame) -> None:
    """Reemplaza el contenido completo de la tabla, igual que `predecir.py`.

    `overwrite` sin filtro borra y escribe en un solo snapshot de Iceberg: nunca hay un momento
    en el que un tablero lea la tabla vacía. La primera corrida usa `append` porque pyiceberg
    avisa por warning cuando el borrado no encuentra nada que borrar.
    """
    arrow = pa.Table.from_pandas(filas, schema=tabla.schema().as_arrow(), preserve_index=False)
    if tabla.current_snapshot() is None:
        tabla.append(arrow)
        return
    tabla.overwrite(arrow)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detecta eventos de pozo en la telemetría")
    parser.add_argument("--horas", type=int, default=24, help="horas de event_time a revisar")
    parser.add_argument("--tabla", default=TABLA, help="tabla destino dentro de gold")
    parser.add_argument("--dry-run", action="store_true", help="no escribe: solo informa")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    config = load_config()
    registro.configurar_artefactos(config)

    mlflow.set_tracking_uri(registro.tracking_uri())
    # Siempre el `champion`: qué versión es la buena lo decide `entrenar_eventos.py` al mover
    # el alias, no un parámetro de esta CLI.
    logger.info("cargando %s desde %s", registro.uri_champion(), registro.tracking_uri())
    modelo = mlflow.sklearn.load_model(registro.uri_champion())
    version = mlflow.MlflowClient().get_model_version_by_alias(registro.MODELO, registro.ALIAS)

    catalogo = datos.abrir_catalogo(config)
    bronze = catalogo.load_table(ORIGEN)
    ultimo = ultimo_event_time(bronze)
    if ultimo is None:
        logger.error("no hay telemetría en lake.%s", ORIGEN)
        return 1
    desde = ultimo - timedelta(hours=args.horas)
    telemetria = leer_telemetria(bronze, desde)
    logger.info(
        "%d lecturas de %d pozos entre %s y %s",
        len(telemetria),
        telemetria["idpozo"].nunique(),
        desde,
        ultimo,
    )

    ventanas = ventanas_por_pozo(telemetria)
    if ventanas.empty:
        logger.warning("no hay ventanas completas de 180 s en el período")
        return 0
    matriz = ventanas[tf.nombres_features()]
    clases = modelo.predict(matriz)
    probabilidades = modelo.predict_proba(matriz).max(axis=1)
    # `pd.Timestamp` y no `datetime.UTC`: esto corre también en el runner, que trae Python 3.10
    # (ADR 0004), donde ese alias todavía no existe.
    momento = pd.Timestamp.now(tz="UTC")
    alertas = armar_alertas(ventanas, clases, probabilidades, f"v{version.version}", momento)

    reparto = pd.Series(clases).value_counts().to_dict()
    logger.info("%d ventanas clasificadas: %s", len(ventanas), reparto)
    if args.dry_run:
        logger.info("dry-run: %d alertas, no se escribe", len(alertas))
        print(alertas.head(10).to_string(index=False))
        return 0

    escribir(asegurar_tabla(catalogo, f"{NAMESPACE}.{args.tabla}"), alertas)
    logger.info(
        "escritas %d alertas en lake.%s.%s | modelo v%s | %d pozos con alerta",
        len(alertas),
        NAMESPACE,
        args.tabla,
        version.version,
        alertas["idpozo"].nunique(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

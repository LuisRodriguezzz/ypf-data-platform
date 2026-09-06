"""Inferencia batch: estima la producción a 12 meses de todos los pozos no convencionales.

El caso de uso real es el pozo que todavía no cumplió el año: se fracturó hace cuatro meses y
la pregunta es cuánto va a acumular. Por eso acá no se filtra por `meses_con_declaracion`
—como sí hace el entrenamiento—, se predice para todos y se guarda al lado el valor real
cuando existe, que es lo que permite medir la deriva del modelo con el tiempo.

El resultado va a `lake.gold.prediccion_produccion_12m`, que se reemplaza entera en cada
corrida: es una foto del modelo vigente sobre el mart vigente, no un histórico.

Uso: `uv run python -m pipelines.ml.predecir`
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestampType

from pipelines.ml import datos, registro
from pipelines.spark_jobs.config import load_config

logger = logging.getLogger("ml.predecir")

TABLA = "prediccion_produccion_12m"

# La tabla es una derivación del mart, no un dato declarado por nadie (ver README).
DATA_ORIGIN = "derived"

COLUMNAS = (
    "idpozo",
    "prod_pet_12m_predicho",
    "prod_pet_12m_real",
    "modelo_version",
    "predicho_en",
    "data_origin",
)


def esquema() -> Schema:
    """Esquema de la tabla de predicciones."""
    return Schema(
        NestedField(1, "idpozo", LongType(), required=False),
        NestedField(2, "prod_pet_12m_predicho", DoubleType(), required=False),
        NestedField(3, "prod_pet_12m_real", DoubleType(), required=False),
        NestedField(4, "modelo_version", StringType(), required=False),
        NestedField(5, "predicho_en", TimestampType(), required=False),
        NestedField(6, "data_origin", StringType(), required=False),
    )


def armar_tabla(
    pozos: pd.DataFrame, prediccion_m3: np.ndarray, version: str, momento: datetime
) -> pd.DataFrame:
    """Una fila por pozo con lo predicho y, si el pozo ya cumplió el año, lo real.

    El real queda nulo cuando faltan meses: no es que el pozo haya producido menos, es que
    todavía no se sabe. Confundir las dos cosas al comparar sería sesgar la evaluación.
    """
    cumplio_el_anio = pozos["meses_con_declaracion"] == datos.MESES_COMPLETOS
    return pd.DataFrame(
        {
            "idpozo": pozos["idpozo"].astype("int64").to_numpy(),
            "prod_pet_12m_predicho": prediccion_m3.astype("float64"),
            "prod_pet_12m_real": pozos[datos.OBJETIVO]
            .astype("float64")
            .where(cumplio_el_anio)
            .to_numpy(),
            "modelo_version": version,
            "predicho_en": momento,
            "data_origin": DATA_ORIGIN,
        },
        columns=list(COLUMNAS),
    )


def asegurar_tabla(catalogo: RestCatalog, identificador: str) -> Table:
    """Crea la tabla la primera vez; después la abre. Sin particiones: son 3.800 filas."""
    catalogo.create_namespace_if_not_exists(datos.NAMESPACE)
    return catalogo.create_table_if_not_exists(identificador, schema=esquema())


def escribir(tabla: Table, filas: pd.DataFrame) -> None:
    """Reemplaza el contenido completo de la tabla por la corrida actual.

    `overwrite` sin filtro borra todo y escribe de nuevo en un solo snapshot de Iceberg: nunca
    hay un momento en el que la tabla se lea vacía. Se distingue la tabla recién creada porque
    pyiceberg avisa por warning cuando el borrado no encuentra nada que borrar; es el mismo
    criterio que `pipelines/reservas/bronze_load.py`.
    """
    arrow = pa.Table.from_pandas(filas, schema=tabla.schema().as_arrow(), preserve_index=False)
    if tabla.current_snapshot() is None:
        tabla.append(arrow)
        return
    tabla.overwrite(arrow)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predice producción a 12 meses por pozo")
    parser.add_argument("--tabla", default=TABLA, help="tabla destino dentro de gold")
    parser.add_argument("--dry-run", action="store_true", help="no escribe: solo informa")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    config = load_config()
    registro.configurar_artefactos(config)

    mlflow.set_tracking_uri(registro.tracking_uri())
    # Siempre el `champion`: qué versión es la buena lo decide `entrenar.py` al mover el alias,
    # no un parámetro de esta CLI. Para volver a una versión anterior se mueve el alias.
    logger.info("cargando %s desde %s", registro.uri_champion(), registro.tracking_uri())
    modelo = mlflow.sklearn.load_model(registro.uri_champion())
    version = mlflow.MlflowClient().get_model_version_by_alias(registro.MODELO, registro.ALIAS)

    catalogo = datos.abrir_catalogo(config)
    pozos = datos.solo_no_convencionales(datos.leer_mart(catalogo))
    preparados = datos.preparar(pozos)
    logger.info("pozos no convencionales: %d", len(preparados))

    prediccion = datos.a_escala_original(modelo.predict(datos.matriz_features(preparados)))
    # timezone.utc y no datetime.UTC: esto también corre en el runner (Python 3.10, ADR 0004).
    momento = datetime.now(timezone.utc).replace(tzinfo=None)
    filas = armar_tabla(preparados, prediccion, f"v{version.version}", momento)

    if args.dry_run:
        logger.info("dry-run: %d filas, no se escribe", len(filas))
        print(filas.head(10).to_string(index=False))
        return 0

    escribir(asegurar_tabla(catalogo, f"{datos.NAMESPACE}.{args.tabla}"), filas)
    con_real = int(filas["prod_pet_12m_real"].notna().sum())
    logger.info(
        "escritas %d filas en lake.%s.%s | modelo v%s | %d con valor real",
        len(filas),
        datos.NAMESPACE,
        args.tabla,
        version.version,
        con_real,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

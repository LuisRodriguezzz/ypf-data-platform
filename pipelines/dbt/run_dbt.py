"""Lanza dbt sobre la misma SparkSession que usan bronze y silver.

dbt-spark con `method: session` no arma la sesión: cada consulta hace
`SparkSession.builder.getOrCreate()` y usa la que encuentre. Este lanzador la crea antes con
`build_spark`, así gold escribe en el catálogo `lake` con exactamente la misma configuración
que las capas de abajo (Iceberg REST + MinIO en local, Glue + S3 en aws).

Se invoca con spark-submit y no con `python3` porque la imagen del runner no trae PySpark
instalado como paquete: vive en /opt/spark/python y es spark-submit el que lo pone en el
PYTHONPATH junto con el gateway de py4j (ADR 0004).

Uso: spark-submit pipelines/dbt/run_dbt.py build --select fact_produccion_mensual
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pipelines.spark_jobs.session import build_spark

PROJECT_DIR = Path(__file__).resolve().parent
# El repo se monta read-only en el runner, así que dbt no puede dejar target/ ni logs/ al lado
# del proyecto: van al volumen persistente que ya se usa para los jars y los paquetes de pip.
ARTIFACTS_DIR = Path(os.environ.get("DBT_ARTIFACTS_DIR", "/home/spark/dbt"))


def configure_environment() -> None:
    """Rutas del proyecto por entorno y no por flag.

    `--project-dir` y `--profiles-dir` habría que repetirlos después del subcomando, y
    `docs generate` son dos palabras: por entorno vale igual para todos los subcomandos.
    """
    os.environ.setdefault("DBT_PROJECT_DIR", str(PROJECT_DIR))
    os.environ.setdefault("DBT_PROFILES_DIR", str(PROJECT_DIR))
    os.environ.setdefault("DBT_TARGET_PATH", str(ARTIFACTS_DIR / "target"))
    os.environ.setdefault("DBT_LOG_PATH", str(ARTIFACTS_DIR / "logs"))


def main(argv: list[str]) -> int:
    configure_environment()
    # Se importa después de fijar el entorno: dbt lee estas variables al importarse.
    from dbt.cli.main import dbtRunner

    spark = build_spark("dbt_gold")
    try:
        result = dbtRunner().invoke(argv)
    finally:
        spark.stop()
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

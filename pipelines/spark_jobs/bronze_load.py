"""Bronze: copia los CSV crudos de landing a Iceberg, una partición por recurso.

Bronze no tipa ni limpia: todas las columnas quedan string y se agregan columnas de
linaje. El tipado y las reglas de calidad son responsabilidad de silver.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.spark_jobs.bronze_rules import (
    LandedFile,
    clean_column_name,
    latest_ok_query,
    namespace_of,
    pending_files,
    postgres_jdbc,
    s3a_uri,
)
from pipelines.spark_jobs.config import load_config
from pipelines.spark_jobs.session import build_spark

# Un dataset por tabla bronze. `reservas` no está: es un XLSX dentro de un ZIP, no un CSV.
DATASET_TABLES = {
    "produccion_pozo": "lake.bronze.produccion_pozo",
    "fractura": "lake.bronze.fractura",
}

logger = logging.getLogger("bronze_load")


def read_manifest(spark: SparkSession, dsn: str, dataset: str) -> list[LandedFile]:
    """Última corrida `ok` de cada recurso, leída por JDBC (sin drivers Python en el runner)."""
    url, properties = postgres_jdbc(dsn)
    rows = spark.read.jdbc(url=url, table=latest_ok_query(dataset), properties=properties).collect()
    return [
        LandedFile(
            resource_id=row["resource_id"],
            resource_name=row["resource_name"],
            landing_key=row["landing_key"],
            sha256=row["sha256"],
            ingest_date=row["ingest_date"].isoformat(),
        )
        for row in rows
        if row["landing_key"]
    ]


def loaded_sha256(spark: SparkSession, table: str) -> dict[str, str]:
    """sha256 ya cargado por recurso. Vacío si la tabla todavía no existe."""
    if not spark.catalog.tableExists(table):
        return {}
    # Cada partición se escribe entera de una vez, así que el sha es único por recurso.
    rows = (
        spark.table(table)
        .groupBy("_resource_id")
        .agg(F.max("_source_sha256").alias("sha256"))
        .collect()
    )
    return {row["_resource_id"]: row["sha256"] for row in rows}


def read_landing_csv(spark: SparkSession, uri: str) -> DataFrame:
    """CSV crudo como strings, con los nombres de columna sin BOM."""
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(uri)
    )
    return df.toDF(*[clean_column_name(name) for name in df.columns])


def with_metadata(df: DataFrame, file: LandedFile) -> DataFrame:
    """Columnas de linaje: de dónde salió la fila y cuándo entró."""
    return (
        df.withColumn("_resource_id", F.lit(file.resource_id))
        .withColumn("_source_key", F.lit(file.landing_key))
        .withColumn("_source_sha256", F.lit(file.sha256))
        .withColumn("_ingest_date", F.to_date(F.lit(file.ingest_date)))
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("data_origin", F.lit("real"))
    )


def write_partition(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Crea la tabla la primera vez; después reemplaza solo la partición del recurso."""
    if spark.catalog.tableExists(table):
        # merge-schema tolera que un año traiga columnas nuevas respecto del primero.
        df.writeTo(table).option("merge-schema", "true").overwritePartitions()
        return
    (
        df.writeTo(table)
        .using("iceberg")
        .partitionedBy(F.col("_resource_id"))
        .tableProperty("write.spark.accept-any-schema", "true")
        .create()
    )


def load_resource(spark: SparkSession, file: LandedFile, bucket: str, table: str) -> int:
    """Carga un recurso y devuelve la cantidad de filas que quedaron en su partición."""
    started = time.monotonic()
    df = with_metadata(read_landing_csv(spark, s3a_uri(bucket, file.landing_key)), file)
    write_partition(spark, df, table)
    rows = spark.table(table).filter(F.col("_resource_id") == file.resource_id).count()
    logger.info(
        "cargado %s | %s filas | %.1f s | %s",
        file.resource_id,
        f"{rows:,}",
        time.monotonic() - started,
        file.resource_name,
    )
    return rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga la capa bronze desde landing")
    parser.add_argument("--dataset", required=True, choices=sorted(DATASET_TABLES))
    parser.add_argument("--resource-id", help="cargar un solo recurso del dataset")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    table = DATASET_TABLES[args.dataset]
    config = load_config()

    spark = build_spark(f"bronze_load:{args.dataset}", config)
    started = time.monotonic()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_of(table)}")
        landed = read_manifest(spark, config.postgres_dsn, args.dataset)
        if args.resource_id:
            landed = [file for file in landed if file.resource_id == args.resource_id]
        pending = pending_files(landed, loaded_sha256(spark, table))
        logger.info(
            "dataset=%s tabla=%s recursos=%d pendientes=%d",
            args.dataset,
            table,
            len(landed),
            len(pending),
        )
        total = sum(
            load_resource(spark, file, config.s3_landing_bucket, table) for file in pending
        )
        logger.info(
            "resumen: %d recursos cargados | %s filas | %.1f s",
            len(pending),
            f"{total:,}",
            time.monotonic() - started,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

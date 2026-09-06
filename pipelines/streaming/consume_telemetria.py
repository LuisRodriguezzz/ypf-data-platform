"""Consumidor: del topic `telemetria_pozo` a Iceberg, con Spark Structured Streaming.

Dos salidas de la misma fuente:
  - `lake.bronze.telemetria_pozo`: todos los eventos crudos, append, particionado por dia
    del `event_time`. Nada se filtra: un evento que llega tarde queda igual en bronze.
  - `lake.silver.telemetria_pozo_1min`: una fila por pozo y ventana de un minuto, con
    watermark de 2 minutos. Los eventos que llegan despues del watermark se descartan de la
    agregacion (Spark los cuenta en `numRowsDroppedByWatermark`) pero siguen en bronze.

Son dos queries independientes, cada una con su checkpoint en `s3a://lakehouse/checkpoints/`
y su propio consumer group: si el job se reinicia, retoma el offset del checkpoint y no
duplica filas en bronze.

Uso: scripts/spark-submit.ps1 pipelines/streaming/consume_telemetria.py --run-for 600
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.spark_jobs.config import LakehouseConfig, load_config
from pipelines.spark_jobs.session import build_spark
from pipelines.streaming.eventos import CAMPOS_EVENTO, SENSORES_CLAVE, columnas_sql

logger = logging.getLogger("streaming.consume_telemetria")

TOPIC = "telemetria_pozo"
BRONZE = "lake.bronze.telemetria_pozo"
SILVER = "lake.silver.telemetria_pozo_1min"
CHECKPOINTS = "s3a://lakehouse/checkpoints"
WATERMARK = "2 minutes"
VENTANA = "1 minute"
# 13 pozos a 60x son ~16.000 eventos por trigger de 20 s; el doble deja margen para
# recuperar atraso sin que un solo batch se vuelva enorme.
MAX_EVENTOS_POR_BATCH = 40000

TIPOS_SPARK = {
    "string": StringType(),
    "long": LongType(),
    "int": IntegerType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
}


def esquema_evento() -> StructType:
    """Esquema explicito del JSON: sin esto Spark tendria que inferirlo en cada batch."""
    return StructType(
        [StructField(nombre, TIPOS_SPARK[tipo], True) for nombre, tipo in CAMPOS_EVENTO]
    )


def crear_tablas(spark: SparkSession) -> None:
    """Las tablas tienen que existir antes de arrancar el sink de streaming."""
    agregados = ", ".join(
        f"{sensor}_avg double, {sensor}_min double, {sensor}_max double"
        for sensor in SENSORES_CLAVE
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {BRONZE} ({columnas_sql()}, _ingested_at timestamp, "
        "data_origin string) USING iceberg PARTITIONED BY (days(event_time))"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {SILVER} (idpozo bigint, ventana_inicio timestamp, "
        f"ventana_fin timestamp, eventos bigint, {agregados}, class_max int, "
        "_ingested_at timestamp, data_origin string) "
        "USING iceberg PARTITIONED BY (days(ventana_inicio))"
    )


def leer_eventos(spark: SparkSession, config: LakehouseConfig) -> DataFrame:
    """Topic -> DataFrame con las columnas del evento (una lectura de pozo por fila)."""
    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", TOPIC)
        # Solo aplica cuando no hay checkpoint: al reiniciar manda el offset guardado.
        .option("startingOffsets", "earliest")
        # Techo de eventos por micro-batch. Sin esto, el primer batch despues de un reinicio
        # se traga todo el atraso de una: un commit gigante que tarda mucho mas y que se
        # pisa con el de la otra query (ver el comentario del trigger en `main`).
        .option("maxOffsetsPerTrigger", MAX_EVENTOS_POR_BATCH)
        .load()
    )
    return crudo.select(
        F.from_json(F.col("value").cast("string"), esquema_evento()).alias("evento")
    ).select("evento.*")


def agregar_por_minuto(eventos: DataFrame) -> DataFrame:
    """Conteo y avg/min/max de los sensores clave por pozo y ventana de un minuto."""
    agregaciones = [F.count(F.lit(1)).alias("eventos")]
    for sensor in SENSORES_CLAVE:
        agregaciones += [
            F.avg(sensor).alias(f"{sensor}_avg"),
            F.min(sensor).alias(f"{sensor}_min"),
            F.max(sensor).alias(f"{sensor}_max"),
        ]
    # max(class) y no avg: interesa si en el minuto hubo algun evento anomalo etiquetado.
    agregaciones.append(F.max("class").alias("class_max"))
    return (
        eventos.withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", VENTANA), "idpozo")
        .agg(*agregaciones)
        .select(
            "idpozo",
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "eventos",
            *[
                f"{sensor}_{medida}"
                for sensor in SENSORES_CLAVE
                for medida in ("avg", "min", "max")
            ],
            "class_max",
            F.current_timestamp().alias("_ingested_at"),
            F.lit("simulated").alias("data_origin"),
        )
    )


def arrancar(df: DataFrame, tabla: str, nombre: str, trigger_s: int):
    """Escribe el DataFrame de streaming en una tabla Iceberg, en modo append."""
    return (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/{nombre}")
        .option("fanout-enabled", "true")
        .trigger(processingTime=f"{trigger_s} seconds")
        .queryName(nombre)
        .toTable(tabla)
    )


def resumen(query) -> tuple[int, int, int]:
    """(filas leidas, filas escritas, filas descartadas por watermark) de la corrida.

    Suma los micro-batches que Spark guarda en `recentProgress` (los ultimos 100): con un
    trigger de 20 s eso es poco mas de media hora, de sobra para una demo.
    """
    leidas = escritas = descartadas = 0
    for progreso in query.recentProgress:
        leidas += progreso.numInputRows
        # El sink de Iceberg no siempre informa filas escritas: manda -1 cuando no las cuenta.
        escritas += max(progreso.sink.numOutputRows, 0)
        for operador in progreso.stateOperators:
            descartadas += operador.numRowsDroppedByWatermark
    return leidas, escritas, descartadas


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume telemetria de Kafka hacia Iceberg")
    parser.add_argument("--trigger", type=int, default=20, help="segundos entre micro-batches")
    parser.add_argument("--run-for", type=int, default=0, help="segundos; 0 = hasta Ctrl-C")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    config = load_config()
    spark = build_spark("consume_telemetria", config)
    # 8 y no las 200 por defecto: con 13 pozos, 200 particiones son 200 archivos diminutos
    # por micro-batch en la tabla silver.
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    crear_tablas(spark)
    bronze = arrancar(
        leer_eventos(spark, config)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("data_origin", F.lit("simulated")),
        BRONZE,
        "telemetria_bronze",
        args.trigger,
    )
    # El catalogo local es SQLite y acepta un solo escritor (ADR 0003): si los dos commits se
    # pisan, el que pierde recibe un SQLITE_BUSY que ni el busy_timeout puede reintentar, el
    # catalogo devuelve 500 y la query muere. Dos medidas para que no coincidan:
    #   - arrancar silver despues, porque el batch caro es el primero (el que recupera atraso);
    #   - un segundo mas de trigger, porque Spark alinea los micro-batches a multiplos
    #     absolutos del intervalo y con el mismo numero coincidirian siempre.
    time.sleep(args.trigger // 2)
    silver = arrancar(
        agregar_por_minuto(leer_eventos(spark, config)),
        SILVER,
        "telemetria_silver_1min",
        args.trigger + 1,
    )
    logger.info(
        "consumiendo %s desde %s | trigger %ds | watermark %s | run-for %ss",
        TOPIC,
        config.kafka_bootstrap_servers,
        args.trigger,
        WATERMARK,
        args.run_for or "sin limite",
    )

    limite = time.monotonic() + args.run_for if args.run_for else None
    fallo = False
    try:
        while bronze.isActive and silver.isActive:
            if limite and time.monotonic() >= limite:
                break
            time.sleep(min(args.trigger, 10))
    except KeyboardInterrupt:
        logger.info("interrumpido")
    finally:
        for query in (bronze, silver):
            leidas, escritas, descartadas = resumen(query)
            logger.info(
                "%s: %s leidas | %s escritas | %s descartadas por watermark",
                query.name,
                f"{leidas:,}",
                f"{escritas:,}",
                f"{descartadas:,}",
            )
            if query.exception():
                fallo = True
                logger.error("%s termino con error: %s", query.name, query.exception())
            query.stop()
        spark.stop()
    return 1 if fallo else 0


if __name__ == "__main__":
    sys.exit(main())

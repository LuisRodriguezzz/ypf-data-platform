"""Construcción de la SparkSession con el catálogo Iceberg REST sobre MinIO."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipelines.spark_jobs.config import LakehouseConfig, load_config

# Los jars (Iceberg, hadoop-aws, JDBC de Postgres) y la memoria del driver se fijan en
# `infra/docker/spark-defaults.conf`: Spark los necesita antes de arrancar la JVM y desde
# acá llegarían tarde. Este módulo solo configura lo que se puede cambiar en caliente.
CATALOG = "lake"
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def build_spark(app_name: str, config: LakehouseConfig | None = None) -> SparkSession:
    """SparkSession `local[*]` con el catálogo `lake` (Iceberg REST) y acceso s3a a MinIO."""
    conf = config or load_config()
    catalog = f"spark.sql.catalog.{CATALOG}"
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "rest")
        .config(f"{catalog}.uri", conf.iceberg_catalog_uri)
        .config(f"{catalog}.warehouse", conf.iceberg_warehouse)
        .config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{catalog}.s3.endpoint", conf.s3_endpoint_url)
        .config(f"{catalog}.s3.path-style-access", "true")
        .config(f"{catalog}.s3.access-key-id", conf.s3_access_key_id)
        .config(f"{catalog}.s3.secret-access-key", conf.s3_secret_access_key)
        .config(f"{catalog}.client.region", conf.s3_region)
        .config("spark.sql.defaultCatalog", CATALOG)
        # s3a se usa solo para leer los CSV de landing; las tablas Iceberg van por S3FileIO.
        .config("spark.hadoop.fs.s3a.endpoint", conf.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", conf.s3_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", conf.s3_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

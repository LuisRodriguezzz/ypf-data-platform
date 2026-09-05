"""Construcción de la SparkSession. El catálogo `lake` cambia según el destino."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipelines.spark_jobs.config import LakehouseConfig, load_config

# En local los jars (Iceberg, hadoop-aws, JDBC de Postgres) y la memoria del driver se fijan
# en `infra/docker/spark-defaults.conf`: Spark los necesita antes de arrancar la JVM y desde
# acá llegarían tarde. En Glue los pone el runtime con `--datalake-formats iceberg`.
CATALOG = "lake"
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def _catalogo_rest(builder, conf: LakehouseConfig):
    """Destino local: catálogo Iceberg REST y objetos en MinIO (endpoint y claves propias)."""
    catalog = f"spark.sql.catalog.{CATALOG}"
    return (
        builder.master("local[*]")
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
        # s3a se usa solo para leer los CSV de landing; las tablas van por S3FileIO.
        .config("spark.hadoop.fs.s3a.endpoint", conf.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", conf.s3_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", conf.s3_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )


def _catalogo_glue(builder, conf: LakehouseConfig):
    """Destino aws: catálogo Glue y objetos en S3 con las credenciales del rol del job.

    Sin endpoint, sin path-style y sin claves: S3FileIO resuelve todo eso solo cuando corre
    dentro de AWS. El master lo fija Glue (YARN), no nosotros.
    """
    catalog = f"spark.sql.catalog.{CATALOG}"
    return (
        builder.config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"{catalog}.warehouse", conf.glue_warehouse)
        .config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    )


def build_spark(app_name: str, config: LakehouseConfig | None = None) -> SparkSession:
    """SparkSession con el catálogo `lake` armado según `LAKEHOUSE_TARGET`."""
    conf = config or load_config()
    builder = SparkSession.builder.appName(app_name).config(
        "spark.sql.extensions", ICEBERG_EXTENSIONS
    )
    builder = _catalogo_glue(builder, conf) if conf.is_aws else _catalogo_rest(builder, conf)
    return builder.config("spark.sql.defaultCatalog", CATALOG).getOrCreate()

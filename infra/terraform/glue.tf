# Catálogo y jobs. El catálogo Spark se sigue llamando `lake` (igual que en local); acá
# `lake.bronze.x` se resuelve contra la base `bronze` del Glue Data Catalog.

resource "aws_glue_catalog_database" "bronze" {
  name        = "bronze"
  description = "Capa bronze: CSV de landing copiados a Iceberg sin tipar."
}

resource "aws_glue_catalog_database" "silver" {
  name        = "silver"
  description = "Capa silver: tablas tipadas según los contratos de pipelines/contracts."
}

locals {
  artifacts_uri = "${local.bucket_uri}/artifacts"

  # Configuración del destino: llega a los jobs como argumentos por defecto y los wrappers
  # de pipelines/aws la exportan a os.environ, así `config.py` no cambia de forma.
  target_arguments = {
    "--LAKEHOUSE_TARGET"  = "aws"
    "--GLUE_WAREHOUSE"    = "${local.bucket_uri}/warehouse"
    "--S3_LANDING_BUCKET" = aws_s3_bucket.lakehouse.bucket
    "--S3_LANDING_PREFIX" = "landing"
    "--S3_REGION"         = var.region
  }

  # Los jars de Iceberg los pone Glue con --datalake-formats: no hay spark.jars.packages.
  # El wheel se instala con pip (--additional-python-modules) y no se pasa por
  # --extra-py-files porque los .yaml de contratos se leen con Path().read_text() y desde
  # un wheel en sys.path (zipimport) no son archivos reales. --no-deps evita traer polars,
  # duckdb y pyiceberg, que los jobs de Spark no usan.
  spark_arguments = merge(local.target_arguments, {
    "--datalake-formats"                = "iceberg"
    "--additional-python-modules"       = "${local.artifacts_uri}/${var.wheel_name},pyyaml==6.0.3"
    "--python-modules-installer-option" = "--no-deps"
    # Job insights escribe métricas y logs extra en CloudWatch que no vamos a mirar.
    "--enable-job-insights" = "false"
  })
}

# Los tres jobs son genéricos: el dataset y el contrato no van en los argumentos por defecto,
# los pasa la máquina de estados de cada pipeline (ver stepfunctions.tf).

# Ingesta: baja los CSV del portal a landing/. Es I/O de red, no necesita Spark, así que va
# como Python shell con 1/16 de DPU (lo más barato que ofrece Glue).
resource "aws_glue_job" "ingest_landing" {
  name         = "ingest_landing"
  description  = "Descarga los recursos de un dataset a landing/ y registra el manifiesto."
  role_arn     = aws_iam_role.glue_job.arn
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 60

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.artifacts_uri}/ingest_job.py"
  }

  default_arguments = merge(local.target_arguments, {
    # Python shell no acepta wheels de S3 en --additional-python-modules, y lo que llega
    # por --extra-py-files lo instala con pip, que rechaza este wheel porque el proyecto
    # pide Python >= 3.11. El script se baja el wheel de acá y lo descomprime a mano.
    "--WHEEL_S3_URI" = "${local.artifacts_uri}/${var.wheel_name}"
    # Versiones mas viejas que las de uv.lock a proposito: Python shell trae 3.9 y
    # psycopg >= 3.3 y pydantic-settings >= 2.10 piden 3.10. Es la unica parte del
    # proyecto que corre en 3.9, y solo usa API estable de esas dos librerias.
    "--additional-python-modules" = "sqlalchemy==2.0.52,psycopg[binary]==3.2.13,pydantic-settings==2.9.1,pyyaml==6.0.3"
    "--library-set"               = "analytics"
    # El DSN es secreto: viaja por SSM, nunca en los argumentos del job.
    "--POSTGRES_DSN_SSM_PARAMETER" = var.postgres_dsn_ssm_parameter
  })
}

resource "aws_glue_job" "bronze_load" {
  name              = "bronze_load"
  description       = "Copia los CSV de landing a las tablas Iceberg de bronze."
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.artifacts_uri}/bronze_job.py"
  }

  default_arguments = merge(local.spark_arguments, {
    "--POSTGRES_DSN_SSM_PARAMETER" = var.postgres_dsn_ssm_parameter
  })
}

resource "aws_glue_job" "silver_load" {
  name              = "silver_load"
  description       = "Aplica un contrato de datos sobre bronze y escribe la tabla silver."
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.artifacts_uri}/silver_job.py"
  }

  default_arguments = local.spark_arguments
}

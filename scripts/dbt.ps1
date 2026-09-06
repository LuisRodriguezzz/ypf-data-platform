# Corre dbt dentro del runner efímero de Spark (ADR 0004, ADR 0009).
# Uso: scripts\dbt.ps1 build
#      scripts\dbt.ps1 run --select dim_pozo
#      scripts\dbt.ps1 docs generate
# Requiere el perfil core levantado: podman-compose --profile core up -d

# `Continue` y no `Stop`: podman-compose escribe avisos en stderr y con `Stop` PowerShell los
# toma como error. El control se hace mirando el codigo de salida del comando.
$ErrorActionPreference = "Continue"
$compose = Join-Path (Split-Path -Parent $PSScriptRoot) "infra\docker\compose.yaml"

# Mismo patron que spark-submit.ps1: se instalan las dependencias (ya cacheadas en el volumen)
# y se lanza el job. dbt va por spark-submit y no por `python3` porque la imagen tiene PySpark
# en /opt/spark/python y es spark-submit el que lo pone en el PYTHONPATH.
# run_dbt.py apunta --project-dir y --profiles-dir a /app/pipelines/dbt por entorno.
$installAndRun = 'python3 -m pip install --user --quiet --disable-pip-version-check ' +
  '-r /app/pipelines/spark_jobs/requirements-runner.txt && ' +
  '/opt/spark/bin/spark-submit /app/pipelines/dbt/run_dbt.py \"$@\"'
podman-compose -f $compose --profile spark run --rm spark bash -c $installAndRun bash @args
exit $LASTEXITCODE

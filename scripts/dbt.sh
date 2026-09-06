#!/usr/bin/env bash
# Corre dbt dentro del runner efímero de Spark (ADR 0004, ADR 0009).
# Uso: scripts/dbt.sh build
#      scripts/dbt.sh run --select dim_pozo
#      scripts/dbt.sh docs generate
# Requiere el perfil core levantado: podman-compose --profile core up -d
set -euo pipefail

# Git Bash reescribe los argumentos que parecen rutas Unix; adentro del contenedor no aplica.
export MSYS_NO_PATHCONV=1

# Se entra al repo y se usa una ruta relativa: podman-compose es un binario de Windows y no
# entiende las rutas estilo /c/Users que arma Git Bash.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# Mismo patrón que spark-submit.sh: dbt va por spark-submit y no por `python3` porque la imagen
# tiene PySpark en /opt/spark/python y es spark-submit el que lo pone en el PYTHONPATH.
# run_dbt.py apunta --project-dir y --profiles-dir a /app/pipelines/dbt por entorno.
exec podman-compose -f infra/docker/compose.yaml --profile spark run --rm spark \
  bash -c 'python3 -m pip install --user --quiet --disable-pip-version-check -r /app/pipelines/spark_jobs/requirements-runner.txt && /opt/spark/bin/spark-submit /app/pipelines/dbt/run_dbt.py "$@"' \
  bash "$@"

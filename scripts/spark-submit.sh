#!/usr/bin/env bash
# Corre spark-submit dentro del runner efímero de Spark (ADR 0004).
# Uso: scripts/spark-submit.sh pipelines/spark_jobs/bronze_load.py --dataset produccion_pozo
# Requiere el perfil core levantado: podman-compose --profile core up -d
set -euo pipefail

# Git Bash reescribe los argumentos que parecen rutas Unix; adentro del contenedor no aplica.
export MSYS_NO_PATHCONV=1

# Se entra al repo y se usa una ruta relativa: podman-compose es un binario de Windows y no
# entiende las rutas estilo /c/Users que arma Git Bash.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# La imagen no tiene /opt/spark/bin en el PATH: se invoca el binario por ruta absoluta.
exec podman-compose -f infra/docker/compose.yaml --profile spark run --rm spark \
  /opt/spark/bin/spark-submit "$@"

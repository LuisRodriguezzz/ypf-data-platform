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
# La imagen no trae las dependencias Python del proyecto (ADR 0004): se instalan con
# `pip install --user` antes de correr el job. Persisten en el volumen `ivy-cache` (montado en
# /home/spark), así que solo la primera corrida las descarga; las siguientes las saltean.
# La imagen no tiene /opt/spark/bin en el PATH: se invoca el binario por ruta absoluta.
# "$@" después de "bash" arma los posicionales del `bash -c` sin pasar por un shell, así que
# los argumentos con espacios llegan intactos sin escaparlos a mano.
exec podman-compose -f infra/docker/compose.yaml --profile spark run --rm spark \
  bash -c 'python3 -m pip install --user --quiet --disable-pip-version-check -r /app/pipelines/spark_jobs/requirements-runner.txt && /opt/spark/bin/spark-submit "$@"' \
  bash "$@"

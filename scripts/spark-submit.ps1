# Corre spark-submit dentro del runner efímero de Spark (ADR 0004).
# Uso: scripts\spark-submit.ps1 pipelines\spark_jobs\bronze_load.py --dataset produccion_pozo
# Requiere el perfil core levantado: podman-compose --profile core up -d

$ErrorActionPreference = "Stop"
$compose = Join-Path (Split-Path -Parent $PSScriptRoot) "infra\docker\compose.yaml"

# La imagen no trae PyYAML (ADR 0004): se instala con `pip install --user` antes de correr el
# job. Persiste en el volumen `ivy-cache` (montado en /home/spark), así que solo la primera
# corrida lo descarga; las siguientes lo saltean.
# La imagen no tiene /opt/spark/bin en el PATH: se invoca el binario por ruta absoluta.
# El script de `bash -c` va en una string de comillas simples (PowerShell no la interpola) con
# `\"$@\"` en vez de `"$@"`: así llega a bash con comillas reales y expande los argumentos de
# @args preservando espacios, sin pasar cada uno por un shell que los reinterprete.
$installAndRun = 'python3 -m pip install --user --quiet --disable-pip-version-check ' +
  '-r /app/pipelines/spark_jobs/requirements-runner.txt && ' +
  '/opt/spark/bin/spark-submit \"$@\"'
podman-compose -f $compose --profile spark run --rm spark bash -c $installAndRun bash @args
exit $LASTEXITCODE

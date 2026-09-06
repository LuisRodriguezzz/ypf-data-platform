# Corre spark-submit dentro del runner efímero de Spark (ADR 0004).
# Uso: scripts\spark-submit.ps1 pipelines\spark_jobs\bronze_load.py --dataset produccion_pozo
# Requiere el perfil core levantado: podman-compose --profile core up -d

# `Continue` y no `Stop`: podman-compose escribe avisos en stderr y con `Stop` PowerShell los
# toma como error, lo que rompe el script cuando lo llama un proceso sin TTY. El control se
# hace mirando el codigo de salida del comando (mismo patron que scripts/aws_deploy.ps1).
$ErrorActionPreference = "Continue"
$compose = Join-Path (Split-Path -Parent $PSScriptRoot) "infra\docker\compose.yaml"

# La imagen no trae las dependencias Python del proyecto (ADR 0004): se instalan con
# `pip install --user` antes de correr el job. Persisten en el volumen `ivy-cache` (montado en
# /home/spark), así que solo la primera corrida las descarga; las siguientes las saltean.
# La imagen no tiene /opt/spark/bin en el PATH: se invoca el binario por ruta absoluta.
# El script de `bash -c` va en una string de comillas simples (PowerShell no la interpola) con
# `\"$@\"` en vez de `"$@"`: así llega a bash con comillas reales y expande los argumentos de
# @args preservando espacios, sin pasar cada uno por un shell que los reinterprete.
$installAndRun = 'python3 -m pip install --user --quiet --disable-pip-version-check ' +
  '-r /app/pipelines/spark_jobs/requirements-runner.txt && ' +
  '/opt/spark/bin/spark-submit \"$@\"'
podman-compose -f $compose --profile spark run --rm spark bash -c $installAndRun bash @args
exit $LASTEXITCODE

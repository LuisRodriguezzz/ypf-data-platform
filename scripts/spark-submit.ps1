# Corre spark-submit dentro del runner efímero de Spark (ADR 0004).
# Uso: scripts\spark-submit.ps1 pipelines\spark_jobs\bronze_load.py --dataset produccion_pozo
# Requiere el perfil core levantado: podman-compose --profile core up -d

$ErrorActionPreference = "Stop"
$compose = Join-Path (Split-Path -Parent $PSScriptRoot) "infra\docker\compose.yaml"

# La imagen no tiene /opt/spark/bin en el PATH: se invoca el binario por ruta absoluta.
podman-compose -f $compose --profile spark run --rm spark /opt/spark/bin/spark-submit @args
exit $LASTEXITCODE

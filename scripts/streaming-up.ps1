# Levanta el lakehouse (core) y el broker de Kafka con su topic (streaming).
# Uso: scripts\streaming-up.ps1        Bajar: scripts\streaming-up.ps1 -Down

param([switch]$Down)

# `Continue` y no `Stop`: podman-compose escribe avisos en stderr (mismo motivo que spark-submit.ps1).
$ErrorActionPreference = "Continue"
$compose = Join-Path (Split-Path -Parent $PSScriptRoot) "infra\docker\compose.yaml"

if ($Down) {
  podman-compose -f $compose --profile streaming stop kafka kafka-init
} else {
  podman-compose -f $compose --profile core --profile streaming up -d
}
exit $LASTEXITCODE

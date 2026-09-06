#!/usr/bin/env bash
# Levanta el lakehouse (core) y el broker de Kafka con su topic (streaming).
# Uso: scripts/streaming-up.sh        Bajar: scripts/streaming-up.sh down
set -euo pipefail

export MSYS_NO_PATHCONV=1
# Ruta relativa: podman-compose es un binario de Windows y no entiende /c/Users.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ "${1:-up}" == "down" ]]; then
  exec podman-compose -f infra/docker/compose.yaml --profile streaming stop kafka kafka-init
fi
exec podman-compose -f infra/docker/compose.yaml --profile core --profile streaming up -d

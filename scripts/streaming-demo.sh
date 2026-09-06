#!/usr/bin/env bash
# Demo de punta a punta: consumidor de Spark en el runner + productor en el host, y al final
# los conteos de las dos tablas. Requiere el perfil streaming levantado (scripts/streaming-up.sh).
# Uso: scripts/streaming-demo.sh [segundos] [velocidad]
set -euo pipefail

SEGUNDOS=${1:-600}
VELOCIDAD=${2:-60}
export MSYS_NO_PATHCONV=1 PYTHONIOENCODING=utf-8
cd "$(dirname "${BASH_SOURCE[0]}")/.."
LOG="${TEMP:-/tmp}/ypf-consume-telemetria.log"

echo "consumidor -> $LOG"
# El consumidor vive 90 s mas que el productor: 45 s los pierde arrancando Spark y el resto
# los necesita para drenar lo que quedo en el topic. Si no, bronze termina con menos filas.
bash scripts/spark-submit.sh pipelines/streaming/consume_telemetria.py \
  --run-for "$((SEGUNDOS + 90))" >"$LOG" 2>&1 &
CONSUMIDOR=$!
# Spark tarda ~40 s en levantar la sesion y crear las tablas antes de leer el topic.
sleep 45

# --loop: las instancias mas cortas de 3W duran ~3 min a 60x; sin repetirlas el caudal cae.
uv run python -m pipelines.streaming.replay_3w --speed "$VELOCIDAD" --run-for "$SEGUNDOS" --loop
# Si una query murio el consumidor sale con 1, pero los conteos se muestran igual.
wait $CONSUMIDOR || echo "el consumidor termino con error, ver $LOG"

grep -E "consumiendo telemetria_pozo|descartadas por watermark" "$LOG" || true
uv run python scripts/check_lake.py --namespace bronze --table telemetria_pozo
uv run python scripts/check_lake.py --namespace silver --table telemetria_pozo_1min

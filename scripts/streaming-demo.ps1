# Demo de punta a punta: consumidor de Spark en el runner + productor en el host, y al final
# los conteos de las dos tablas. Requiere el perfil streaming levantado (scripts\streaming-up.ps1).
# Uso: scripts\streaming-demo.ps1 -Segundos 600 -Velocidad 60

param([int]$Segundos = 600, [double]$Velocidad = 60)

$ErrorActionPreference = "Continue"
$env:PYTHONIOENCODING = "utf-8"
$repo = Split-Path -Parent $PSScriptRoot
$log = Join-Path $env:TEMP "ypf-consume-telemetria.log"

Write-Host "consumidor -> $log"
# El consumidor vive 90 s mas que el productor: 45 s los pierde arrancando Spark y el resto
# los necesita para drenar lo que quedo en el topic. Si no, bronze termina con menos filas.
$consumidor = Start-Job -ScriptBlock {
  param($repo, $log, $segundos)
  Set-Location $repo
  & "$repo\scripts\spark-submit.ps1" pipelines/streaming/consume_telemetria.py --run-for $segundos *> $log
} -ArgumentList $repo, $log, ($Segundos + 90)

# Spark tarda ~40 s en levantar la sesion y crear las tablas antes de leer el topic.
Start-Sleep -Seconds 45
# --loop: las instancias mas cortas de 3W duran ~3 min a 60x; sin repetirlas el caudal cae.
uv run python -m pipelines.streaming.replay_3w --speed $Velocidad --run-for $Segundos --loop
Wait-Job $consumidor | Out-Null
Remove-Job $consumidor

Select-String -Path $log -Pattern "consumiendo telemetria_pozo|descartadas por watermark" |
  ForEach-Object { $_.Line }
uv run python scripts/check_lake.py --namespace bronze --table telemetria_pozo
uv run python scripts/check_lake.py --namespace silver --table telemetria_pozo_1min

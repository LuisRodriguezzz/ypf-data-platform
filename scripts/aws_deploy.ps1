# Sube los artefactos que ejecutan los jobs de Glue: el wheel del proyecto y los tres
# wrappers de pipelines/aws/. La infraestructura la crea Terraform; esto solo publica codigo.
# Uso: scripts\aws_deploy.ps1

# `Continue` y no `Stop`: uv y aws escriben progreso en stderr y con `Stop` PowerShell lo
# toma como error. El control se hace mirando el codigo de salida de cada comando.
$ErrorActionPreference = "Continue"
$repo = Split-Path -Parent $PSScriptRoot
$terraform = Join-Path $repo "infra\terraform"

function Invoke-O-Fallar {
  param([string]$Que)
  if ($LASTEXITCODE -ne 0) { throw "$Que fallo con codigo $LASTEXITCODE" }
}

# La CLI de AWS no siempre esta en el PATH.
$aws = (Get-Command aws -ErrorAction SilentlyContinue).Source
if (-not $aws) { $aws = "C:\Program Files\Amazon\AWSCLIV2\aws.exe" }

Write-Host "== uv build =="
uv build --wheel --project $repo
Invoke-O-Fallar "uv build"
$wheel = Get-ChildItem (Join-Path $repo "dist\*.whl") | Sort-Object LastWriteTime | Select-Object -Last 1

$bucket = terraform -chdir="$terraform" output -raw lakehouse_bucket
Invoke-O-Fallar "terraform output"
$destino = "s3://$bucket/artifacts"

Write-Host "== subiendo a $destino =="
& $aws s3 cp $wheel.FullName "$destino/$($wheel.Name)"
Invoke-O-Fallar "aws s3 cp del wheel"
foreach ($script in "ingest_job.py", "bronze_job.py", "silver_job.py") {
  & $aws s3 cp (Join-Path $repo "pipelines\aws\$script") "$destino/$script"
  Invoke-O-Fallar "aws s3 cp de $script"
}

Write-Host ""
Write-Host "wheel:   $destino/$($wheel.Name)"
Write-Host "scripts: $destino/{ingest_job.py,bronze_job.py,silver_job.py}"
Write-Host "Si el nombre del wheel cambio, actualiza la variable wheel_name de Terraform."

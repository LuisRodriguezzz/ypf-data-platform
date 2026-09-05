#!/usr/bin/env bash
# Sube los artefactos que ejecutan los jobs de Glue: el wheel del proyecto y los tres
# wrappers de pipelines/aws/. La infraestructura la crea Terraform; esto solo publica codigo.
# Uso: scripts/aws_deploy.sh
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="$repo/infra/terraform"

# En Git Bash la CLI de AWS puede no estar en el PATH.
aws_cli="$(command -v aws || echo "/c/Program Files/Amazon/AWSCLIV2/aws.exe")"

echo "== uv build =="
uv build --wheel --project "$repo"
wheel="$(ls -t "$repo"/dist/*.whl | head -1)"

bucket="$(terraform -chdir="$terraform_dir" output -raw lakehouse_bucket)"
destino="s3://$bucket/artifacts"

echo "== subiendo a $destino =="
"$aws_cli" s3 cp "$wheel" "$destino/$(basename "$wheel")"
for script in ingest_job.py bronze_job.py silver_job.py; do
  "$aws_cli" s3 cp "$repo/pipelines/aws/$script" "$destino/$script"
done

echo
echo "wheel:   $destino/$(basename "$wheel")"
echo "scripts: $destino/{ingest_job.py,bronze_job.py,silver_job.py}"
echo "Si el nombre del wheel cambio, actualiza la variable wheel_name de Terraform."

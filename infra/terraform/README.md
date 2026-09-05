# Destino `aws`

Infraestructura mínima para correr los pipelines en AWS: un bucket S3, el Glue Data Catalog,
tres jobs de Glue, una máquina de estados por pipeline y un workgroup de Athena. El porqué de
cada elección está en `docs/adr/0008-destino-aws-glue-step-functions.md`.

Los tres jobs (`ingest_landing`, `bronze_load`, `silver_load`) son genéricos: no tienen
dataset ni contrato en sus argumentos por defecto, se los pasa la máquina de estados. Cada
pipeline es una entrada del mapa `local.pipelines` de `stepfunctions.tf` con su dataset, su
contrato y su cron: agregar uno nuevo son cinco líneas, no otra definición de estados.

El state es **local** a propósito: el entorno es efímero y lo maneja una sola persona.
`*.tfstate` ya está en el `.gitignore` del repo.

## Desplegar

```powershell
terraform init
terraform plan
terraform apply
..\..\scripts\aws_deploy.ps1    # uv build + sube wheel y scripts a artifacts/
```

`aws_deploy.ps1` lee el bucket de `terraform output` y publica el wheel del proyecto y los
tres wrappers de `pipelines/aws/`. Los jobs leen su script de `s3://<bucket>/artifacts/` en
cada corrida: después de tocar código alcanza con volver a correrlo, sin `terraform apply`.
Si cambia la versión del proyecto, actualizar también la variable `wheel_name`.

Requisito externo: el parámetro SecureString `/ypf-lakehouse/postgres_dsn` con la cadena de
conexión a Neon. Se crea a mano y no lo maneja Terraform: es un secreto.

## Correr el pipeline

```powershell
$arn = (terraform output -json state_machine_arns | ConvertFrom-Json).fractura_diaria
# Corrida completa (el dataset y el contrato los pone la máquina de estados)
aws stepfunctions start-execution --state-machine-arn $arn --input '{}'
# Corrida acotada: se mezcla con los argumentos fijos del pipeline, no los reemplaza
aws stepfunctions start-execution --state-machine-arn $arn --input '{\"ingesta\":{\"--only\":\"^Padr\"},\"silver\":{\"--contract\":\"pozo_primera_produccion\"}}'

aws stepfunctions describe-execution --execution-arn <arn de la ejecución>
aws glue get-job-runs --job-name bronze_load --max-items 1
..\..\scripts\aws_logs.ps1    # resumen de la última corrida de cada job
```

Los schedules de EventBridge (uno por pipeline) existen pero nacen **deshabilitados**: nada
queda corriendo solo. Para habilitarlos, `terraform apply -var enable_schedule=true`.

## Consultar en Athena

```powershell
aws athena start-query-execution --work-group ypf-lakehouse `
  --query-string "SELECT count(*) FROM silver.pozo_primera_produccion"
aws athena get-query-results --query-execution-id <id>
```

El workgroup fuerza su propia ubicación de resultados (`athena-results/`, que se limpia a
los 7 días), así que no hace falta pasar `--result-configuration`.

## Destruir

```powershell
terraform destroy
```

Borra el bucket **con todos los datos adentro** (`force_destroy = true`): landing, las
tablas Iceberg de bronze y silver, los artefactos y los resultados de Athena. También borra
las bases del catálogo, los tres jobs, las máquinas de estados, los schedules, el workgroup
y los tres roles de IAM. No toca el parámetro de SSM ni la base de Neon: el manifiesto de
ingesta sobrevive, así que una reconstrucción hay que arrancarla desde landing vacío.

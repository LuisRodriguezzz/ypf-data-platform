# Destino `aws`

Infraestructura mínima para correr las cuatro capas en AWS: un bucket S3, el Glue Data
Catalog, cinco jobs de Glue, una máquina de estados por pipeline y un workgroup de Athena. El
porqué de cada elección está en `docs/adr/0008-destino-aws-glue-step-functions.md` y, para
gold, en `docs/adr/0010-gold-en-aws-con-dbt-athena.md`.

| Job | Tipo | Qué hace |
| --- | --- | --- |
| `ingest_landing` | Python shell 3.9, 1/16 DPU | Baja los recursos de un dataset a `landing/`. |
| `bronze_load` | Glue 5.0 Spark, 2 × G.1X | CSV de landing a las tablas Iceberg de bronze. |
| `bronze_reservas` | Python shell 3.9, 1 DPU | Parsea el XLSX anual de reservas y escribe `bronze.reservas` con pyiceberg. |
| `silver_load` | Glue 5.0 Spark, 2 × G.1X | Aplica un contrato y escribe la tabla silver. |
| `gold_dbt` | Glue 5.0, 2 × G.1X | `dbt build --target aws`; el SQL lo ejecuta Athena. |

Los cuatro primeros son genéricos: no tienen dataset ni contrato en sus argumentos por
defecto, se los pasa la máquina de estados. Cada pipeline es una entrada del mapa
`local.pipelines` de `stepfunctions.tf` con su dataset, su contrato, **qué job usa para
bronze** y su cron: agregar uno nuevo son seis líneas, no otra definición de estados.

El state es **local** a propósito: el entorno es efímero y lo maneja una sola persona.
`*.tfstate` ya está en el `.gitignore` del repo.

## Desplegar

```powershell
terraform init
terraform plan
terraform apply
..\..\scripts\aws_deploy.ps1    # uv build + sube wheel y wrappers a artifacts/
```

`aws_deploy.ps1` lee el bucket de `terraform output` y publica el wheel del proyecto y todos
los `pipelines/aws/*_job.py`. Los jobs leen su script de `s3://<bucket>/artifacts/` en cada
corrida: después de tocar código alcanza con volver a correrlo, sin `terraform apply`. Si
cambia la versión del proyecto, actualizar también la variable `wheel_name`.

El proyecto de dbt (`pipelines/dbt/`) viaja adentro del wheel, así que `gold_dbt` también se
actualiza con `aws_deploy`: un modelo nuevo no necesita Terraform.

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

Las máquinas son `produccion_pozo_mensual`, `fractura_diaria`, `reservas_mensual` y
`gold_mensual`. Los jobs de Glue admiten una corrida a la vez, así que dos pipelines de
fuente no pueden ir en paralelo: comparten `ingest_landing` y `silver_load`.

Los schedules de EventBridge (uno por máquina) existen pero nacen **deshabilitados**: nada
queda corriendo solo. Para habilitarlos, `terraform apply -var enable_schedule=true`.

## Consultar en Athena

```powershell
aws athena start-query-execution --work-group ypf-lakehouse `
  --query-string "SELECT count(*) FROM silver.pozo_primera_produccion"
aws athena get-query-results --query-execution-id <id>
```

El workgroup fuerza su propia ubicación de resultados (`athena-results/`, que se limpia a
los 7 días), así que no hace falta pasar `--result-configuration`. Las tablas de gold no
viven ahí: dbt las escribe en `warehouse/gold/` justamente para que la regla de ciclo de vida
no se las lleve.

## Destruir

```powershell
terraform plan -destroy   # qué se lleva puesto, sin llevárselo
terraform destroy
```

Hoy son 29 recursos. Borra el bucket **con todos los datos adentro** (`force_destroy = true`): landing, las
tablas Iceberg de bronze, silver y gold, los artefactos y los resultados de Athena. También
borra las tres bases del catálogo, los cinco jobs, las máquinas de estados, los schedules, el
workgroup y los tres roles de IAM. No toca el parámetro de SSM ni la base de Neon: el
manifiesto de ingesta sobrevive.

## Reconstruir el entorno de cero

Es el procedimiento completo, tal como se probaría después de un `destroy`. Toma alrededor de
una hora, casi toda esperando a que la ingesta baje los CSV, y cuesta menos de 2 USD.

```powershell
cd infra\terraform
terraform destroy                # ver arriba qué se lleva puesto
terraform apply
..\..\scripts\aws_deploy.ps1     # wheel + wrappers a artifacts/

# El manifiesto de ingesta sigue en Neon y dice que todo está descargado, pero landing
# quedó vacío: hay que olvidarlo para que la ingesta vuelva a bajar los archivos.
# (Desde el host, con el DSN de Neon en el entorno.)
uv run python -c "from pipelines.ingest.manifest import Manifest, ingestion_manifest; import os; m = Manifest(os.environ['POSTGRES_DSN']); c = m.engine.connect(); c.execute(ingestion_manifest.delete()); c.commit()"

$maquinas = terraform output -json state_machine_arns | ConvertFrom-Json
foreach ($nombre in "produccion_pozo_mensual", "fractura_diaria", "reservas_mensual", "gold_mensual") {
  aws stepfunctions start-execution --state-machine-arn $maquinas.$nombre --input '{}'
  # Esperar a que termine antes de la siguiente: los jobs no corren en paralelo.
}
```

El orden importa: `gold_mensual` al final, porque `mart_pozo_completacion_produccion` cruza
producción con fractura y con el padrón de pozos. Si alguna fuente falta, el mart sale corto
y los tests de relación entre hechos y dimensiones lo delatan.

Verificación, con `scripts/aws_logs.ps1` para los logs y con Athena para las filas:

```sql
-- Medido el 2026-09-06. Fractura y producción crecen con cada republicación del portal;
-- reservas y el mart no, porque el ZIP anual y el padrón de pozos ya están cerrados.
SELECT count(*) FROM silver.produccion_pozo;          -- 18.218.514
SELECT count(*) FROM silver.pozo_primera_produccion;  --     86.197
SELECT count(*) FROM silver.fractura;                 --      4.878
SELECT count(*) FROM silver.reservas;                 --    198.734
SELECT count(*) FROM gold.mart_pozo_completacion_produccion;  -- 4.635
```

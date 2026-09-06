# Destino `aws`

Infraestructura mínima para correr las cuatro capas en AWS: un bucket S3, el Glue Data
Catalog, cinco jobs de Glue, una máquina de estados por pipeline y un workgroup de Athena. El
porqué de cada elección está en `docs/adr/0008-destino-aws-glue-step-functions.md` y, para
gold, en `docs/adr/0010-gold-en-aws-con-dbt-athena.md`.

| Job | Tipo | Qué hace |
| --- | --- | --- |
| `ingest_landing` | Python shell 3.9, 1/16 DPU | Baja los recursos de un dataset a `landing/`. |
| `bronze_load` | Glue 5.0 Spark, N × G.1X | CSV de landing a las tablas Iceberg de bronze. |
| `bronze_reservas` | Python shell 3.9, 1 DPU | Parsea el XLSX anual de reservas y escribe `bronze.reservas` con pyiceberg. |
| `silver_load` | Glue 5.0 Spark, N × G.1X | Aplica un contrato y escribe la tabla silver. |
| `gold_dbt` | Glue 5.0, N × G.1X | `dbt build --target aws`; el SQL lo ejecuta Athena. |

Los cuatro primeros son genéricos: no tienen dataset ni contrato en sus argumentos por
defecto, se los pasa la máquina de estados. Cada pipeline es una entrada del mapa
`local.pipelines` de `stepfunctions.tf` con su dataset, su contrato, **qué job usa para
bronze** y su cron: agregar uno nuevo son seis líneas, no otra definición de estados.

## Ambientes

Hay dos, `dev` y `prod`, en la misma cuenta (ADR 0014). Los mismos `.tf` para los dos: la
variable `environment` sufija el nombre de cada recurso y el aislamiento del state lo da un
**workspace de Terraform** por ambiente.

| | dev | prod |
| --- | --- | --- |
| Bucket | `ypf-lakehouse-<cuenta>-dev` | `ypf-lakehouse-<cuenta>-prod` |
| Bases de Glue | `bronze_dev`, `silver_dev`, `gold_dev` | `bronze_prod`, `silver_prod`, `gold_prod` |
| Jobs / máquinas | `bronze_load_dev`, `fractura_diaria_dev` | `bronze_load_prod`, `fractura_diaria_prod` |
| Roles / workgroup | `ypf-data-platform-glue-job-dev`, `ypf-lakehouse-dev` | `…-prod` |
| Workers de Spark | 2 (el mínimo de Glue) | 4 |
| Schedules | deshabilitados | deshabilitados |
| DSN de Neon | `/ypf-lakehouse/dev/postgres_dsn` (branch `dev`) | `/ypf-lakehouse/prod/postgres_dsn` (branch `main`) |

`environment` **no tiene default**: siempre hay que pasar el `-var-file`. Y el workspace
tiene que coincidir con el ambiente del tfvars: `aws_s3_bucket.lakehouse` lleva una
`precondition` que corta el plan si no coinciden, con el comando que hay que correr.

El sufijo llega al código por una sola variable de entorno, `GLUE_DATABASE_SUFFIX`, que
Terraform pasa como argumento a cada job. Los jobs de Spark la aplican con
`bronze_rules.with_suffix` sobre los nombres que leen de los YAML de contratos, y dbt la
compone en `profiles.yml` (`schema: "gold{{ env_var('GLUE_DATABASE_SUFFIX', '') }}"`) y en
`models/sources.yml`. Vacía —el destino local— deja todos los nombres como estaban.

El state es **local** todavía, un archivo por workspace en `terraform.tfstate.d/`. El bloque
de backend S3 está escrito y comentado en `versions.tf`, y el bucket y la tabla de locks
están definidos en [`bootstrap/`](bootstrap/README.md), que **no se aplicó**.

## Desplegar

```powershell
cd infra\terraform
terraform init
terraform workspace select -or-create dev      # o prod
terraform plan  -var-file=envs\dev.tfvars
terraform apply -var-file=envs\dev.tfvars
..\..\scripts\aws_deploy.ps1                   # uv build + sube wheel y wrappers a artifacts/
```

`aws_deploy.ps1` lee el bucket y el ambiente de `terraform output` **del workspace
seleccionado**: publica en el ambiente en el que uno esté parado, y lo imprime antes de subir
nada. Los jobs leen su script de `s3://<bucket>/artifacts/` en cada corrida: después de tocar
código alcanza con volver a correrlo, sin `terraform apply`. Si cambia la versión del
proyecto, actualizar también la variable `wheel_name`.

El proyecto de dbt (`pipelines/dbt/`) viaja adentro del wheel, así que `gold_dbt` también se
actualiza con `aws_deploy`: un modelo nuevo no necesita Terraform.

Requisito externo por ambiente: el parámetro SecureString `/ypf-lakehouse/<ambiente>/postgres_dsn`
con la cadena de conexión al branch de Neon correspondiente. Se crean a mano y no los maneja
Terraform: son secretos.

```powershell
# Una vez por ambiente, con el DSN del branch de Neon que corresponda.
aws ssm put-parameter --name /ypf-lakehouse/dev/postgres_dsn --type SecureString `
  --value "postgresql://..." --overwrite
```

## Despliegue automático (deshabilitado)

`.github/workflows/deploy.yml` hace, cuando está habilitado: `terraform plan` de dev en cada
PR que toque `infra/terraform/**` o `pipelines/**`, `apply` de dev en cada push a `main` con
subida del wheel, y un job de prod que depende del de dev y espera aprobación manual. Se
autentica con OIDC (`role-to-assume`), sin claves en los secretos del repo.

Está deshabilitado por diseño: todos los jobs llevan `if: vars.DEPLOY_ENABLED == 'true'` y esa
variable no existe. **No puede funcionar mientras el state sea local**: un runner de GitHub
arranca vacío, no ve `terraform.tfstate.d/` y creería que no existe nada. Los cinco pasos para
habilitarlo están en [`bootstrap/README.md`](bootstrap/README.md).

## Migrar el despliegue que hay hoy

Hoy hay un solo despliegue, sin sufijo (bucket `ypf-lakehouse-<cuenta>`, jobs `bronze_load`,
bases `bronze`/`silver`/`gold`, roles `ypf-data-platform-*`, workgroup `ypf-lakehouse`), con
su `terraform.tfstate` en el workspace `default`. Con estos `.tf` ya no se puede aplicar
encima: falta `environment` y todos los nombres cambian.

### Qué se recrea (spoiler: todo)

En AWS **el nombre es la identidad** de casi todo lo que hay acá. No existe "renombrar":
Terraform borra y crea. De los 29 recursos:

| Recurso | ¿Se recrea? | Qué se pierde |
| --- | --- | --- |
| `aws_s3_bucket.lakehouse` (+ versioning, cifrado, lifecycle, public access block) | Sí | **Todos los objetos**: `landing/`, `warehouse/` (bronze, silver y gold), `artifacts/`, `athena-results/`. Con `force_destroy = true` se van sin preguntar. |
| `aws_glue_catalog_database` ×3 | Sí | La metadata de todas las tablas Iceberg (la base se borra con sus tablas). |
| `aws_glue_job` ×5 | Sí | Nada: el script vive en S3 y el historial de corridas no se usa. |
| `aws_sfn_state_machine` ×4 | Sí | El historial de ejecuciones (90 días). |
| `aws_scheduler_schedule` ×4 | Sí | Nada: están deshabilitados. |
| `aws_iam_role` ×3 (+ 3 políticas inline + 1 attachment) | Sí | Nada. |
| `aws_athena_workgroup` | Sí | El historial de consultas. |

Lo que **no** toca ninguna de las dos opciones: el manifiesto de ingesta en Neon y los
parámetros de SSM.

### Paso previo común a las dos opciones: mudar el state al workspace `prod`

El state actual está en el workspace `default`. Las dos opciones empiezan igual:

```powershell
cd infra\terraform
terraform workspace new prod                          # crea terraform.tfstate.d\prod\
copy terraform.tfstate terraform.tfstate.d\prod\terraform.tfstate
terraform plan -var-file=envs\prod.tfvars             # 29 a reemplazar
```

Nota sobre `terraform state mv`: acá **no hace falta**, porque las direcciones de Terraform no
cambian (`aws_s3_bucket.lakehouse` se sigue llamando igual en el código); lo que cambia es el
atributo `name` de cada recurso. Lo que se mueve es el archivo de state entero, del workspace
`default` al `prod`. Un `state mv` entre direcciones haría falta solo si además se hubieran
renombrado los bloques del `.tf`.

Con el state ya en el workspace `prod`, la `precondition` del bucket queda conforme
(`terraform.workspace == var.environment`) y el plan corre. Muestra los 29 recursos con
`must be replaced`, cada uno con el motivo: `name` / `bucket` `forces replacement`.

### Opción A — `terraform apply` que renombra, conservando los datos con `aws s3 sync`

La idea sería copiar el bucket viejo al nuevo para no perder las tablas. **No funciona**, y
conviene saber por qué antes de intentarlo:

1. **Los metadatos de Iceberg guardan rutas absolutas.** Cada `metadata.json` y cada manifest
   de `warehouse/` dice `s3://ypf-lakehouse-<cuenta>/warehouse/...`. Un
   `aws s3 sync s3://ypf-lakehouse-<cuenta>/ s3://ypf-lakehouse-<cuenta>-prod/` copia los
   archivos pero deja todos esos punteros apuntando al bucket viejo, que ya no existe. Las
   tablas de bronze, silver y gold no se leerían: habría que reescribir los metadatos o volver
   a registrar cada tabla a mano.
2. **Las bases de Glue también se recrean**, y una base se borra con sus tablas: aunque los
   archivos estuvieran bien, el catálogo quedaría vacío igual.
3. **El `apply` destruye el bucket antes de crear el nuevo** (no hay `create_before_destroy`),
   así que el `sync` tendría que ir a un bucket temporal y volver después: dos copias
   completas del warehouse, ida y vuelta, para terminar con tablas que no abren.

Lo único que sí sobrevive a un `sync` es `landing/`: son CSV y ZIP crudos, sin rutas adentro,
y las claves que guarda el manifiesto de Neon son relativas al bucket. Ese pedazo de la opción
A es el que vale la pena, y está incorporado en la opción B.

### Opción B — aplicar prod limpio y reconstruir · **recomendada**

Es la misma "reconstrucción de cero" que el proyecto ya tiene probada y medida: **alrededor
de una hora y menos de 2 USD**, casi todo esperando a que la ingesta baje los CSV. A cambio se
queda con un `prod` coherente, sin metadatos remendados.

```powershell
cd infra\terraform

# 0. Guardar landing/ a disco local (unos 3 GB; tarda unos minutos). El bucket destino
#    todavía no existe, así que la copia no puede ir directo de bucket a bucket.
aws s3 sync s3://ypf-lakehouse-<cuenta>/landing/ .\landing-backup\

# 1. Mudar el state al workspace prod (ver arriba) y revisar el plan.
terraform workspace new prod
copy terraform.tfstate terraform.tfstate.d\prod\terraform.tfstate
terraform plan -var-file=envs\prod.tfvars

# 2. Aplicar: destruye los 29 recursos viejos y crea los 29 con el sufijo -prod.
#    El bucket viejo se va con todo adentro (force_destroy = true).
terraform apply -var-file=envs\prod.tfvars
..\..\scripts\aws_deploy.ps1            # imprime el ambiente antes de subir

# 3. Devolver landing/ al bucket nuevo.
$bucket = terraform output -raw lakehouse_bucket
aws s3 sync .\landing-backup\ "s3://$bucket/landing/"

# 4. Crear el parámetro de SSM del ambiente, con el DSN del branch `main` de Neon.
#    El de antes (/ypf-lakehouse/postgres_dsn) ya no lo lee nadie.
aws ssm put-parameter --name /ypf-lakehouse/prod/postgres_dsn --type SecureString `
  --value "postgresql://..." --overwrite

# 5. Correr los cuatro pipelines, de a uno (los jobs de Glue no van en paralelo).
#    Como landing/ está poblado y el manifiesto de Neon sigue intacto, la ingesta no vuelve
#    a bajar nada: ve los mismos sha256 y no hace trabajo. Bronze y silver sí reconstruyen
#    las tablas Iceberg en el bucket nuevo y en las bases con sufijo.
$maquinas = terraform output -json state_machine_arns | ConvertFrom-Json
foreach ($nombre in "produccion_pozo_mensual", "fractura_diaria", "reservas_mensual", "gold_mensual") {
  aws stepfunctions start-execution --state-machine-arn $maquinas.$nombre --input '{}'
  # Esperar a que termine antes de la siguiente.
}
```

Si en el paso 0 se salteara la copia de `landing/`, hay que además vaciar el manifiesto
(`ingestion_manifest`) antes del paso 5, como en "Reconstruir el entorno de cero": si no, la
ingesta cree que ya está todo descargado y bronze se queda sin archivos. El comando está más
abajo. Sin el atajo, la migración pasa de ~1 hora a ~1 hora y media (la descarga del portal es
la parte lenta).

**Por qué B y no A**: A conservaría los objetos pero no las tablas, que es lo único que
interesaba conservar. B tarda una hora, cuesta menos de 2 USD y deja todo consistente. El
proyecto ya paga ese precio cada vez que se demuestra desde cero, y esta migración se hace una
sola vez.

### Y dev, después

Una vez que prod esté arriba, dev es un `apply` más, con el mismo procedimiento pero
partiendo de vacío:

```powershell
terraform workspace new dev
terraform apply -var-file=envs\dev.tfvars
..\..\scripts\aws_deploy.ps1
# Crear /ypf-lakehouse/dev/postgres_dsn con el DSN del branch `dev` de Neon.
# Correr un solo pipeline para probar: fractura_diaria (la fuente más chica, ~5 MB).
```

## Correr el pipeline

```powershell
$arn = (terraform output -json state_machine_arns | ConvertFrom-Json).fractura_diaria
# Corrida completa (el dataset y el contrato los pone la máquina de estados)
aws stepfunctions start-execution --state-machine-arn $arn --input '{}'
# Corrida acotada: se mezcla con los argumentos fijos del pipeline, no los reemplaza
aws stepfunctions start-execution --state-machine-arn $arn --input '{\"ingesta\":{\"--only\":\"^Padr\"},\"silver\":{\"--contract\":\"pozo_primera_produccion\"}}'

aws stepfunctions describe-execution --execution-arn <arn de la ejecución>
aws glue get-job-runs --job-name bronze_load_prod --max-items 1
..\..\scripts\aws_logs.ps1    # resumen de la última corrida de cada job
```

Las claves de `state_machine_arns` son los nombres sin sufijo (`fractura_diaria`) aunque la
máquina se llame `fractura_diaria_prod`: así estos comandos valen igual en los dos ambientes.
Los pipelines son `produccion_pozo_mensual`, `fractura_diaria`, `reservas_mensual` y
`gold_mensual`. Los jobs de Glue admiten una corrida a la vez, así que dos pipelines de
fuente no pueden ir en paralelo: comparten `ingest_landing` y `silver_load`.

Los schedules de EventBridge (uno por máquina) existen pero nacen **deshabilitados** en los
dos ambientes: nada queda corriendo solo y el costo en reposo es cero. Para habilitarlos,
cambiar `enable_schedule` en el tfvars del ambiente y volver a aplicar.

## Consultar en Athena

```powershell
aws athena start-query-execution --work-group ypf-lakehouse-prod `
  --query-string "SELECT count(*) FROM silver_prod.pozo_primera_produccion"
aws athena get-query-results --query-execution-id <id>
```

El workgroup fuerza su propia ubicación de resultados (`athena-results/`, que se limpia a
los 7 días), así que no hace falta pasar `--result-configuration`. Las tablas de gold no
viven ahí: dbt las escribe en `warehouse/gold/` justamente para que la regla de ciclo de vida
no se las lleve.

## Destruir

```powershell
terraform workspace select dev
terraform plan -destroy -var-file=envs\dev.tfvars   # qué se lleva puesto, sin llevárselo
terraform destroy -var-file=envs\dev.tfvars
```

Son 29 recursos por ambiente. Borra el bucket **con todos los datos adentro**
(`force_destroy = true`): landing, las tablas Iceberg de bronze, silver y gold, los artefactos
y los resultados de Athena. También borra las tres bases del catálogo, los cinco jobs, las
máquinas de estados, los schedules, el workgroup y los tres roles de IAM. No toca el parámetro
de SSM ni la base de Neon: el manifiesto de ingesta sobrevive.

Destruir un ambiente no toca al otro: son dos states distintos.

## Reconstruir el entorno de cero

Es el procedimiento completo, tal como se probaría después de un `destroy`. Toma alrededor de
una hora, casi toda esperando a que la ingesta baje los CSV, y cuesta menos de 2 USD.

```powershell
cd infra\terraform
terraform workspace select prod
terraform destroy -var-file=envs\prod.tfvars     # ver arriba qué se lleva puesto
terraform apply   -var-file=envs\prod.tfvars
..\..\scripts\aws_deploy.ps1                     # wheel + wrappers a artifacts/

# El manifiesto de ingesta sigue en Neon y dice que todo está descargado, pero landing
# quedó vacío: hay que olvidarlo para que la ingesta vuelva a bajar los archivos.
# (Desde el host, con el DSN del branch de Neon del ambiente en el entorno.)
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
-- Medido el 2026-09-06, sobre el despliegue único de entonces (bases sin sufijo).
-- Fractura y producción crecen con cada republicación del portal; reservas y el mart no,
-- porque el ZIP anual y el padrón de pozos ya están cerrados.
SELECT count(*) FROM silver.produccion_pozo;          -- 18.218.514
SELECT count(*) FROM silver.pozo_primera_produccion;  --     86.197
SELECT count(*) FROM silver.fractura;                 --      4.878
SELECT count(*) FROM silver.reservas;                 --    198.734
SELECT count(*) FROM gold.mart_pozo_completacion_produccion;  -- 4.635
```

# ADR 0008 — El destino aws corre en Glue, orquestado por Step Functions

**Estado:** aceptada · 2026-09-05

## Contexto

El destino `aws` del ADR 0001 tiene que correr el mismo pipeline que el local (ingesta,
bronze, silver sobre Iceberg) con un presupuesto de 5 USD y créditos de plan gratuito. La
restricción no es técnica sino económica: cualquier recurso que quede prendido —un NAT
Gateway, una instancia de RDS, un clúster de EMR, un entorno de MWAA— consume el
presupuesto entero sin que nadie ejecute nada.

## Decisión

**Spark en Glue y no en EMR.** Glue cobra por DPU-hora consumida y no deja nada corriendo
entre ejecuciones: los tres jobs juntos cuestan centavos por corrida. Un clúster EMR, aun
transitorio, factura las instancias mientras está levantado y agrega tiempo de arranque.
Glue 5.0 además trae los jars de Iceberg con `--datalake-formats iceberg`, así que no hay
que resolver dependencias de la JVM como en el runner local (ADR 0004). El catálogo pasa a
ser Glue Data Catalog: en local es REST + MinIO, en AWS es `GlueCatalog` + S3, y lo único
que cambia en el código es la función que arma la SparkSession.

**Step Functions y no MWAA.** Un entorno de MWAA cuesta del orden de 350 USD al mes esté
o no corriendo un DAG: es la opción más cara de todo el proyecto. Step Functions cobra por
transición de estado (los primeros 4.000 pasos mensuales son gratis) y el DAG que hay que
reproducir son tres tareas en serie. La máquina de estados usa `glue:startJobRun.sync`, que
espera a que cada job termine y falla la ejecución si el job falla: la misma semántica que
las dependencias del DAG de Airflow. Airflow sigue siendo el orquestador de referencia y
vive en local (ADR 0006); Step Functions es el equivalente en la nube, no un reemplazo.

**Neon y no RDS.** El manifiesto de ingesta necesita Postgres. La instancia más chica de
RDS cuesta unos 12 USD al mes corriendo todo el día, y apagarla entre corridas la deja
igual pagando el almacenamiento. Neon da un Postgres serverless gratis con escalado a cero,
que es exactamente el patrón de uso: unas pocas consultas por corrida mensual. La cadena de
conexión vive en SSM Parameter Store como SecureString y los jobs reciben el *nombre* del
parámetro, nunca el valor: un secreto en los argumentos de un job queda visible en la
consola y en `get-job-runs`.

**State de Terraform local.** El entorno es efímero: se crea, se demuestra y se destruye.
Un backend remoto pediría un bucket y una tabla de locks que sobrevivirían al `destroy` y
costarían plata para nada, y no hay un segundo operador con quien coordinar. `*.tfstate` ya
está en el `.gitignore`.

## Consecuencias

- El costo del entorno en reposo es cero: S3 con unos pocos MB y nada más. Solo se paga
  cuando alguien dispara la máquina de estados.
- El schedule de EventBridge nace deshabilitado (`enable_schedule = false`). Se habilita a
  propósito cuando se quiere dejar el pipeline corriendo solo.
- Si se pierde el `terraform.tfstate` hay que reimportar o destruir a mano. Es el precio
  aceptado por no sostener infraestructura para el propio Terraform.
- Perder la cuenta de Neon deja el manifiesto sin backend: los datos de landing siguen en
  S3 pero bronze no sabe qué cargar. La reconstrucción es correr la ingesta de nuevo.

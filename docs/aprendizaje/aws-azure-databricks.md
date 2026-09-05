# AWS, Azure y Databricks para un ingeniero de datos: guía de estudio sobre el proyecto de pozos de petróleo de Argentina

> Fecha de referencia: 2026-09-05. Todas las cifras de precios están tomadas de las páginas oficiales de AWS/Microsoft/Databricks citadas en cada afirmación. Cuando no se pudo confirmar un dato oficial con exactitud, se aclara explícitamente en el texto en lugar de inventarlo.

## Cómo está armado este documento

Está pensado para acompañar el proyecto real: un pipeline de datos públicos de pozos de petróleo de Argentina que en local corre con MinIO + catálogo REST de Iceberg + Spark en contenedor + Airflow, y en AWS corre con S3, Glue Data Catalog, Glue ETL (Spark 5.0) + Glue Python shell para la ingesta, Step Functions con `glue:startJobRun.sync`, EventBridge Scheduler, Athena, IAM, SSM Parameter Store y CloudWatch Logs, todo con Terraform y un manifiesto de ingesta en Postgres (Neon).

---

## 1. Mapa mental: los 6 roles de una plataforma de datos

Cualquier plataforma de datos moderna —AWS, Azure, Databricks o el laboratorio local del proyecto— resuelve seis problemas. Conviene pensarlo por rol, no por nombre de servicio: los nombres cambian, los roles no.

1. **Almacenamiento de objetos** (el data lake físico).
2. **Catálogo de metadatos** (qué tablas existen, su esquema, su ubicación).
3. **Cómputo batch/distribuido** (Spark u otro motor que transforma datos a escala).
4. **Orquestación** (qué corre, en qué orden, con qué reintentos y qué schedule).
5. **Consulta SQL** (motor para que analistas y BI consulten el lake sin mover datos).
6. **Seguridad, secretos y observabilidad** (identidades, credenciales, logs y métricas).

| Rol | AWS | Azure (nativo) | Databricks | Entorno local del proyecto |
|---|---|---|---|---|
| Almacenamiento | Amazon S3 | ADLS Gen2 (sobre Blob Storage) [Microsoft Learn](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) | DBFS / Volumes (sobre ADLS, S3 o GCS) | MinIO (S3-compatible) |
| Catálogo | Glue Data Catalog | Microsoft Purview / metastore de Fabric | Unity Catalog [Databricks Docs](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) | Catálogo REST de Iceberg |
| Cómputo batch | Glue ETL (Spark), EMR, Lambda, ECS Fargate | Azure Databricks, Synapse Spark, Fabric Spark, Azure Functions | Clusters clásicos, serverless, Lakeflow Declarative Pipelines | Spark en contenedor |
| Orquestación | Step Functions, EventBridge Scheduler, Glue Workflows, MWAA | Data Factory / Synapse Pipelines / Fabric Data Pipelines, Logic Apps | Jobs / Lakeflow Jobs [Databricks Docs](https://docs.databricks.com/en/jobs/index.html) | Airflow |
| Consulta SQL | Athena | Synapse Serverless SQL, Fabric SQL endpoint | SQL Warehouse [Databricks Docs](https://docs.databricks.com/en/compute/sql-warehouse/index.html) | Spark SQL / Trino ad hoc |
| Seguridad/secretos/observabilidad | IAM, SSM Parameter Store, Secrets Manager, CloudWatch Logs | Microsoft Entra ID, Azure Key Vault [Microsoft Learn](https://learn.microsoft.com/en-us/azure/key-vault/general/overview), Azure Monitor, Purview | Unity Catalog access control, Databricks Secrets [Databricks Docs](https://docs.databricks.com/en/security/secrets/index.html), system tables | variables de entorno / `.env` local |

Idea central: en AWS estos roles están repartidos en servicios separados que hay que cablear a mano (de ahí Step Functions, IAM, etc.); en Databricks, Unity Catalog y Lakeflow buscan unificar catálogo + gobierno + orquestación + cómputo en un mismo plano de control; Azure está a mitad de camino y migrando su propia orquestación (Data Factory → Fabric).

---

## 2. Un capítulo por servicio de AWS

### 2.1 Amazon S3

**Qué es en una frase**: almacenamiento de objetos durable y prácticamente ilimitado, la base física de cualquier data lake.

**Conceptos clave**: buckets, objects, prefijos (no hay carpetas reales, son claves con `/`), clases de almacenamiento (Standard, Intelligent-Tiering, Standard-IA, One Zone-IA, Express One Zone, Glacier Instant Retrieval, Glacier Flexible Retrieval, Glacier Deep Archive) [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/).

**Cómo se cobra**: por GB almacenado/mes (según clase y región), por requests, y por transferencia saliente. La página oficial [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/) es dinámica y no expuso el dígito exacto en este fetch; la cifra ampliamente documentada para S3 Standard en us-east-1 es **≈USD 0,023/GB-mes para los primeros 50 TB** — tomala como aproximada y verificala en la calculadora oficial antes de presupuestar. Nuevos clientes reciben hasta USD 200 en créditos de Free Tier válidos 6 meses [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/).

**Límites relevantes**: sin límite de tamaño de bucket ni de cantidad de objetos; objetos individuales hasta 5 TB.

**Cuándo usarlo / cuándo no**: para archivos inmutables baratos (Parquet, JSON crudo, backups), no para acceso transaccional de baja latencia. Acá es el destino de la capa raw/bronze/silver/gold, reemplazando a MinIO local.

**Equivalente en Azure**: ADLS Gen2, técnicamente Blob Storage con namespace jerárquico habilitado, no un servicio separado [Microsoft Learn](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction). Diferencia real: tiene carpetas reales, con `rename`/`delete` de directorio como operación atómica de metadatos — algo que S3 no tiene (ahí "renombrar" un prefijo implica copiar objeto por objeto).

**Equivalente en Databricks**: Volumes (dentro de Unity Catalog), almacenamiento gobernado con rutas tipo sistema de archivos sobre S3/ADLS/GCS; Databricks no reemplaza el storage, se sienta encima.

---

### 2.2 AWS Glue Data Catalog

**Qué es en una frase**: el metastore técnico central de AWS, compatible como reemplazo directo de Apache Hive Metastore [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html).

**Conceptos clave**: un catálogo por cuenta y por región; databases y tables dentro de cada catálogo; crawlers que escanean S3 y generan/actualizan tablas automáticamente; particiones.

**Cómo se cobra**: el primer millón de objetos almacenados y el primer millón de requests por mes son gratis; por encima, USD 1,00 por cada 100.000 objetos adicionales por mes. Operaciones de estadísticas y compactación de tablas Iceberg se cobran a USD 0,44 por DPU-hora [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/).

**Límites relevantes**: es compartido por Athena, Redshift Spectrum, EMR y el propio Glue — punto de integración entre todos esos servicios [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html).

**Cuándo usarlo / cuándo no**: conviene centralizarlo siempre que uses Athena, EMR o Glue en la misma cuenta. No sirve como gobierno fino por columna/fila (para eso está Lake Formation encima). En este proyecto reemplaza al catálogo REST de Iceberg local: los Glue ETL jobs escriben tablas Iceberg ahí y Athena las consulta.

**Equivalente en Azure**: no hay 1:1; Fabric trae su propio metastore de Lakehouse y Purview cubre gobierno/catalogación organizacional, pero no es un Hive Metastore drop-in.

**Equivalente en Databricks**: Unity Catalog, con un namespace de tres niveles `catalog.schema.tabla` en vez de los dos niveles (`database.tabla`) de Glue, y con control de acceso, linaje y auditoría integrados en la misma capa [Databricks Docs](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) — Glue Data Catalog en cambio delega ese control a IAM + Lake Formation por separado.

---

### 2.3 AWS Glue ETL jobs (Spark)

**Qué es en una frase**: Spark administrado y serverless, facturado por DPU-hora, sin clusters que mantener.

**Conceptos clave**:
- **AWS Glue version** fija las versiones de Spark y Python disponibles (3.0, 4.0, 5.0…) [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html).
- **Worker types**: G.1X = 1 DPU (4 vCPU, 16 GB RAM, ~44 GB disco libre); G.2X = 2 DPU (8 vCPU, 32 GB RAM, ~78 GB libres); también G.4X, G.8X, G.12X, G.16X y la familia R memory-optimized [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html).
- **DPU (Data Processing Unit)**: unidad de cómputo = 4 vCPU + 16 GB de RAM [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html).
- **`--datalake-formats`**: parámetro que habilita Iceberg/Hudi/Delta en el runtime de Spark; para Iceberg se setea en `iceberg` sumando la configuración de Spark del catálogo Iceberg-Glue [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html). La versión de Iceberg soportada depende de la versión de Glue: 5.0 trae Iceberg 1.7.1, 4.0 trae 1.0.0 [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html).
- **Job bookmarks**: estado incremental que evita reprocesar datos ya vistos; funciona con JDBC, el transform Relationalize y fuentes S3 (Parquet/ORC desde Glue 1.0+) [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html). Se puede pausar, resetear o rebobinar para backfills.
- **Auto scaling**: desde Glue 3.0+, agrega/quita workers según el paralelismo real de cada etapa hasta un máximo configurado; solo con worker types G/R, no con DPU fijas [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/auto-scaling.html).

**Cómo se cobra**: USD 0,44 por DPU-hora, facturado por segundo [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/). Ejemplo oficial: 15 minutos con 6 DPU ≈ USD 0,66.

**Límites relevantes**: timeout máximo 7 días (10.080 min); default 2.880 min en Glue 4.0 o anterior, y **480 min en Glue 5.0 en adelante** [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html) — importante si tus jobs Glue 5.0 son largos.

**Cuándo usarlo / cuándo no**: ideal para ETL batch en S3 sin gestionar infraestructura, integrado nativamente al Data Catalog. No conviene para jobs sub-minuto de alta frecuencia (el arranque de Spark tarda; ahí va Lambda) ni cargas que necesitan control fino de la topología del cluster (ahí EMR da más control). Acá, dos de los tres jobs Glue Spark 5.0 hacen las transformaciones silver/gold leyendo/escribiendo Iceberg vía `--datalake-formats iceberg`.

**Equivalente en Azure**: Azure Databricks o Synapse Spark pools — ninguno tiene el modelo "DPU-hora fijo con worker types predefinidos" de Glue; ambos configuran el cluster con más libertad pero también más responsabilidad de tuning.

**Equivalente en Databricks**: clusters clásicos (job clusters efímeros) o serverless jobs compute; y para pipelines declarativos, Lakeflow Declarative Pipelines (ex Delta Live Tables), framework declarativo en SQL/Python sobre Spark en vez de escribir el DAG a mano [Databricks Docs](https://docs.databricks.com/aws/en/dlt/).

---

### 2.4 AWS Glue Python shell jobs

**Qué es en una frase**: ejecución de scripts Python "planos" (sin Spark) para tareas ligeras como ingesta, llamadas a APIs o escritura de manifiestos.

**Conceptos clave**: soporta Python 3.6 (deprecado desde el 1 de marzo de 2026) o 3.9 [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html); DPU configurable en **0,0625 o 1 DPU** (default 0,0625), con 20 GB de disco local en ambos casos y ~14 GiB libres en `/tmp` [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html); trae librerías precargadas (`boto3`, `pandas`, `awswrangler`, `pyathena`, etc. en el set "analytics") o se puede usar `--additional-python-modules` para instalar vía pip.

**Cómo se cobra**: la página de precios de Glue no discrimina una tarifa separada para Python shell; sigue el mismo modelo de USD 0,44 por DPU-hora [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/) — con 0,0625 DPU eso equivale a unos USD 0,0275 por hora de ejecución.

**Límites relevantes**: **no soporta job bookmarks** [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html); no admite `.egg` en Python 3.9+ (usar `.whl`); no soporta `--extra-files`.

**Cuándo usarlo / cuándo no**: perfecto para ingesta simple (llamar una API pública, escribir un manifiesto en Postgres) sin necesidad de Spark; no sirve para transformaciones distribuidas de volúmenes grandes. Acá corre el job de ingesta que lee el manifiesto de Postgres (Neon) y baja los datos públicos de pozos.

**Equivalente en Azure**: Azure Functions (consumo por invocación) o un Container App Job; no hay un "shell Python administrado y DPU-facturado" 1:1.

**Equivalente en Databricks**: un task tipo "Python script" o "Python wheel" dentro de un Job, corriendo en un cluster de un solo nodo (o serverless), sin el concepto de DPU fraccionaria — se factura por DBU y tiempo de cluster.

---

### 2.5 AWS Step Functions

**Qué es en una frase**: orquestador serverless de propósito general basado en máquinas de estado, no acoplado a ningún servicio en particular.

**Conceptos clave**:
- **State machine** definida en **Amazon States Language (ASL)**, JSON declarativo; también soporta **JSONata** para transformar datos como alternativa a los payload templates clásicos [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language.html).
- **Standard vs Express**: Standard corre hasta 1 año, semántica *exactly-once*, historial consultable hasta 90 días, y es la única que soporta `.sync` y `.waitForTaskToken`; Express corre hasta 5 minutos, semántica *at-least-once*/*at-most-once*, sin historial nativo (hay que mandarlo a CloudWatch Logs) [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-standard-vs-express.html).
- **Integraciones `.sync`**: el patrón "Run a Job" espera a que el servicio de destino termine antes de avanzar; Glue, Athena, Batch, ECS/Fargate, EKS, EMR, EMR Serverless y SageMaker lo soportan, entre otros [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html).

**Cómo se cobra**: Standard, USD 0,000025 por transición de estado, con free tier de 4.000 transiciones/mes que no vence (indefinido, no solo los primeros 12 meses) [AWS Step Functions Pricing](https://aws.amazon.com/step-functions/pricing/). Express se cobra por millón de requests (USD 1,00) más duración en GB-segundo (USD 0,00001667/GB-s, memoria facturada en bloques de 64 MB) [AWS Step Functions Pricing](https://aws.amazon.com/step-functions/pricing/).

**Límites relevantes**: el tipo de workflow es inmutable una vez creado [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-standard-vs-express.html); Standard tiene límites de tasa de transición de estado que pueden requerir aumento de cuota en cargas muy paralelas.

**Cuándo usarlo / cuándo no**: ideal para encadenar servicios AWS heterogéneos con reintentos, catch de errores, paralelismo (`Map`/`Parallel`) y esperar resultados (`.sync`). No reemplaza a Airflow si necesitás backfills declarativos por intervalo de fecha, sensores con polling configurable, o un DAG versionado como código Python testeable. Acá, la máquina de estados Standard encadena los tres Glue jobs (ingesta Python shell → transformaciones Spark) con `arn:aws:states:::glue:startJobRun.sync` en cada Task state, de forma que cada paso espera al anterior [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html); su rol IAM necesita como mínimo `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns` y `glue:BatchStopJobRun` [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html).

**Equivalente en Azure**: sin mapeo 1:1; lo más cercano conceptualmente es Logic Apps (integración de eventos con conectores, no pensado para "esperar a que termine un job de Spark"); para encadenar ETL, Azure usa directamente Data Factory/Fabric Pipelines.

**Equivalente en Databricks**: Lakeflow Jobs, con tasks encadenadas por dependencias, `if/else`, `for each` y el task type `run_job` para invocar otro Job y esperar su resultado [Databricks Docs](https://docs.databricks.com/en/jobs/index.html) — el equivalente más directo a `.sync`, pero dentro del mismo plano de control de Databricks.

---

### 2.6 Amazon EventBridge Scheduler

**Qué es en una frase**: programador serverless centralizado para disparar tareas por cron/rate o de una sola vez, sin necesidad de mantener infraestructura [AWS Scheduler Docs](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html).

**Conceptos clave**: schedules y schedule groups; "templated targets" (SQS, SNS, Lambda, EventBridge) o "universal target" para 270+ servicios y 6.000+ operaciones de API; ventanas de tiempo flexibles; reintentos con entrega *at-least-once* [AWS Scheduler Docs](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html).

**Cómo se cobra**: free tier de **14.000.000 de invocaciones por mes**; por encima, USD 1,00 por millón de invocaciones [AWS EventBridge Pricing](https://aws.amazon.com/eventbridge/pricing/).

**Cuándo usarlo / cuándo no**: perfecto para arrancar la máquina de estados de Step Functions con un cron simple; no es un orquestador en sí — solo dispara, no encadena ni maneja dependencias. Acá dispara el `StartExecution` de Step Functions con un cron diario.

**Equivalente en Azure**: los triggers de tiempo (`Schedule trigger`) dentro de Data Factory/Fabric Pipelines cumplen el mismo rol pero integrados al propio orquestador, no como servicio separado.

**Equivalente en Databricks**: los triggers de tipo *Scheduled* de un Job (cron) o los *file arrival triggers*, que además pueden reaccionar a la llegada de archivos nuevos en el storage [Databricks Docs](https://docs.databricks.com/en/jobs/index.html).

---

### 2.7 Amazon Athena

**Qué es en una frase**: motor de consultas SQL serverless sobre datos en S3, sin necesidad de cargar los datos a ningún lado.

**Conceptos clave**: workgroups para controlar costo y versión de motor por equipo/uso; el "Athena engine version 3" está construido con base en el motor open source **Trino** (y Presto), incorporando mejoras de esos proyectos de forma continua [AWS Athena Docs](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html).

**Cómo se cobra**: USD 5 por TB escaneado en consultas SQL estándar; reservas de capacidad a USD 0,30/DPU-hora; Athena for Apache Spark, USD 0,35/DPU-hora [AWS Athena Pricing](https://aws.amazon.com/athena/pricing/). Consultas exitosas y fallidas que llegan a escanear S3 generan cargo de datos escaneados y tarifas estándar de S3 [AWS Athena Pricing](https://aws.amazon.com/athena/pricing/).

**Soporte de Iceberg**: Athena lee, hace time travel, escribe (`INSERT`, no `UPDATE`) y ejecuta DDL sobre tablas Iceberg **v2** (solo crea/opera v2), únicamente si están registradas en Glue Data Catalog, con locking optimista de Glue, y Parquet/ORC/Avro como formatos soportados en el motor v3 [AWS Athena Docs](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html).

**Límites relevantes**: la sintaxis de time travel cambió en engine v3 a `FOR TIMESTAMP AS OF` / `FOR VERSION AS OF` (reemplazando `SYSTEM_TIME`/`SYSTEM_VERSION`) [AWS Athena Docs](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html); el timestamp de Iceberg admite microsegundos pero Athena solo soporta milisegundos en lectura/escritura [AWS Athena Docs](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html).

**Cuándo usarlo / cuándo no**: ideal para consultas ad hoc y BI sobre el lake sin levantar un warehouse; no conviene para consultas repetitivas de alta concurrencia con SLA estricto (ahí un warehouse dedicado tipo Redshift, o un SQL Warehouse de Databricks). Acá es la capa de consulta final sobre las tablas Iceberg gold, reemplazando las consultas ad hoc de Spark SQL/Trino locales.

**Equivalente en Azure**: Synapse Serverless SQL pool (pago por TB escaneado) o el SQL endpoint de Fabric Lakehouse.

**Equivalente en Databricks**: SQL Warehouse — no se paga por TB escaneado sino por tiempo de cómputo (DBU/hora) con escalado elástico serverless [Databricks Docs](https://docs.databricks.com/en/compute/sql-warehouse/index.html), un modelo de costo distinto según el patrón de uso.

---

### 2.8 IAM roles y políticas mínimas

**Qué es en una frase**: el mecanismo de identidad de AWS que le da permisos temporales a servicios, sin credenciales de larga duración [AWS IAM Docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html).

**Conceptos clave**: un rol tiene una **trust policy** (quién puede asumirlo — el "assume role") y una o más **permission policies** (qué puede hacer una vez asumido) [AWS IAM Docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html); a diferencia de un usuario IAM, un rol no tiene contraseña ni access keys permanentes, solo credenciales temporales de sesión; **role chaining** (asumir un rol desde otro rol) limita la sesión a 1 hora sin importar la duración máxima configurada del rol [AWS IAM Docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html).

**Cómo lo usa este proyecto**: cada Glue job tiene su rol de ejecución con permisos mínimos (S3 de su prefijo, Data Catalog, SSM); la máquina de estados tiene el suyo, solo con los permisos `glue:StartJobRun`/`GetJobRun`/`GetJobRuns`/`BatchStopJobRun` mencionados arriba. **Least privilege** es el principio detrás de todo esto — dar a cada rol exactamente los permisos que necesita, ni más; Terraform lo hace auditable al versionar la política como código.

**Equivalente en Azure**: Microsoft Entra ID (ex Azure AD) para identidades, con **Managed Identity** como equivalente conceptual de un rol IAM, y Azure RBAC para las políticas de permisos.

**Equivalente en Databricks**: Unity Catalog usa sus propios *grants* SQL (`GRANT SELECT ON TABLE ... TO ...`) sobre principals que a su vez pueden mapear a identidades de Entra ID/IAM del cloud subyacente; a nivel de cluster/job también existen *service principals* de Databricks.

---

### 2.9 SSM Parameter Store vs Secrets Manager

**Qué son en una frase**: dos almacenes de configuración/secretos de AWS con modelos de costo opuestos — uno casi gratis para configuración simple, el otro pago pero con rotación automática de secretos.

**Cómo se cobra**:
- **Parameter Store**: los parámetros **estándar son gratis**; los **avanzados cuestan USD 0,05 por parámetro por mes** (prorrateado por hora); las interacciones de API a través del tier de throughput estándar son gratis para parámetros estándar, y **USD 0,05 por cada 10.000 interacciones** de API para parámetros avanzados o para el tier de throughput superior [AWS Systems Manager Pricing](https://aws.amazon.com/systems-manager/pricing/).
- **Secrets Manager**: **USD 0,40 por secreto por mes** más **USD 0,05 por cada 10.000 llamadas a la API** [AWS Secrets Manager Pricing](https://aws.amazon.com/secrets-manager/pricing/).

**Cuándo usar cada uno**: Parameter Store para configuración no rotativa (endpoints, nombres de bucket, flags) donde el costo cero importa; Secrets Manager para rotación automática de credenciales (contraseñas de bases de datos, API keys de terceros). Acá, SSM guarda configuración no sensible del pipeline (bucket, rutas, tablas Glue), probablemente dentro del free tier de parámetros estándar dado el volumen chico del proyecto.

**Equivalente en Azure**: Azure Key Vault cubre ambos casos en un solo servicio (secretos, claves de cifrado y certificados), con tiers Standard (software, FIPS 140 nivel 1) y Premium (HSM, FIPS 140-3 nivel 3) [Microsoft Learn](https://learn.microsoft.com/en-us/azure/key-vault/general/overview) — no separa "parámetros baratos" de "secretos rotables" como AWS.

**Equivalente en Databricks**: Databricks Secrets, organizados en *secret scopes* con permisos tipo ACL, cifrados en una base gestionada por Databricks; no tiene un modelo de costo por secreto — el uso está incluido en el plan del workspace [Databricks Docs](https://docs.databricks.com/en/security/secrets/index.html).

---

### 2.10 Amazon CloudWatch Logs

**Qué es en una frase**: el servicio central de logs de AWS, donde caen por defecto los logs de Glue, Step Functions (si se habilita) y Lambda.

**Conceptos clave**: log groups (uno por job/función) que contienen log streams (uno por ejecución); retención configurable por grupo.

**Cómo se cobra**: ingesta a **USD 0,50 por GB** por encima del free tier (con precios escalonados más bajos para "vended logs" de muy alto volumen, bajando hasta USD 0,05/GB por encima de 50 TB); almacenamiento archivado a **USD 0,03 por GB por mes**; free tier de **5 GB combinados** (ingesta + almacenamiento + escaneo de Logs Insights) [AWS CloudWatch Pricing](https://aws.amazon.com/cloudwatch/pricing/).

**Cuándo usarlo / cuándo no**: para debugging y auditoría de corto/mediano plazo; para retención larga y barata conviene exportar a S3 en vez de dejarlo indefinidamente acá (el storage archivado sale más caro que S3 Glacier). Acá caen los logs de los tres Glue jobs y, si se habilita logging en la máquina de estados, el historial de Step Functions Express (obligatorio) o Standard (opcional).

**Equivalente en Azure**: Azure Monitor / Log Analytics workspace.

**Equivalente en Databricks**: system tables (`system.query.history`, tablas de auditoría, de lineage) más el logging nativo de driver/executor de cada cluster, accesible vía UI o exportable a un storage externo.

---

## 3. La pregunta central: ¿Step Functions es un orquestador real o un workaround?

Esta es la pregunta que más confunde a quien viene de Airflow. La respuesta corta: **Glue Workflows/Triggers NO es un orquestador general — es orquestación interna de Glue. Step Functions SÍ es un orquestador general, y es lo que AWS recomienda para encadenar Glue con otros servicios.**

### Glue Workflows y Triggers

Un **workflow de Glue** agrupa jobs, crawlers y triggers de Glue como una sola entidad, corrible on-demand o programada [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/orchestrate-using-workflows.html), con triggers por schedule, on-demand o encadenados (job A termina, dispara job B). El límite es claro: **solo orquesta recursos de Glue** — no puede esperar a un Lambda, un ECS task, ni decidir en base a la salida de un servicio externo. Sirve solo cuando **todo** el pipeline vive dentro de Glue.

### Step Functions como orquestador de propósito general

Step Functions no es específico de ningún servicio: tiene integraciones optimizadas con más de 20 servicios (Glue, Athena, Batch, ECS/EKS, EMR, EMR Serverless, Lambda, SageMaker, SNS, SQS, y sí mismo) y, vía SDK integrations, puede llamar a más de 200 servicios de AWS con cualquier acción de su API [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html). El patrón recomendado por la documentación oficial para encadenar Glue jobs es el integration pattern `.sync` (`arn:aws:states:::glue:startJobRun.sync`): cada Task state espera a que el `JobRun` de Glue termine antes de avanzar [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html) — exactamente lo que hace este proyecto para encadenar los tres Glue jobs.

### MWAA (Amazon Managed Workflows for Apache Airflow)

MWAA es Airflow completamente administrado por AWS, cobrado por **entorno**: entorno pequeño **USD 0,49/hora**, grande **USD 0,99/hora**, más workers adicionales (USD 0,055–0,22/hora), servidor web adicional (USD 0,11/hora en grande) y almacenamiento de base de datos (USD 0,10/GB-mes) [AWS MWAA Pricing](https://aws.amazon.com/managed-workflows-for-apache-airflow/pricing/). Eso implica un **piso de costo mensual fijo** aunque no corra nada, a diferencia de Step Functions, que no cobra si no hay transiciones — la razón principal para descartarlo en un proyecto de portfolio con pocos jobs diarios.

Según el blog de AWS, MWAA conviene con **dependencias complejas de datos, jobs de horas/días, necesidad de reanudar desde el último punto exitoso, y expertise previo en Airflow**; Step Functions conviene para **workflows serverless orientados a eventos, tareas de minutos a horas, integración pesada con AWS, aprobación humana y tráfico variable** [AWS Big Data Blog](https://aws.amazon.com/blogs/big-data/choosing-the-right-workflow-orchestration-service-for-your-use-case-amazon-mwaa-and-aws-step-functions/). No son mutuamente excluyentes: hay organizaciones que usan MWAA como orquestador principal y delegan sub-workflows a Step Functions.

### Comparación honesta: Airflow vs Step Functions vs Glue Workflows

| Dimensión | Apache Airflow / MWAA | AWS Step Functions | Glue Workflows/Triggers |
|---|---|---|---|
| Definición del DAG | Código Python, testeable con pytest | JSON/YAML declarativo (ASL), editor visual | Triggers vía consola/API, sin "código" de flujo |
| Backfills / data intervals | Nativo: `data_interval_start/end`, catchup, backfill por rango de fechas | Sin concepto nativo; hay que modelarlo a mano con inputs | No aplica — reprocesa según bookmarks de cada job |
| Reintentos | Por tarea, backoff configurable, alertas | Por estado, `Retry`/`Catch` declarativo en ASL | A nivel de job individual de Glue |
| Sensores | Nativos (esperar archivo, otro DAG, poke/reschedule) | Sin sensor nativo; se simula con polling + `Wait` o `.sync` | Ninguno; depende de triggers de eventos |
| UI | Rica: Gantt, logs por tarea, grafo, re-run parcial | Diagrama de ejecución y replay visual | Vista de workflow en consola de Glue, limitada |
| Costo | Piso fijo por entorno (desde ~USD 0,49/h) [AWS MWAA Pricing](https://aws.amazon.com/managed-workflows-for-apache-airflow/pricing/) | Pay-per-transición, sin piso; free tier indefinido de 4.000/mes [AWS Step Functions Pricing](https://aws.amazon.com/step-functions/pricing/) | Incluido en el costo de jobs/crawlers orquestados |
| Curva de aprendizaje | Media-alta (framework, operators, providers) | Media (ASL/JSONata, integration patterns) | Baja, pero limitada a Glue |
| Integración no-AWS | Excelente: providers para casi todo (Snowflake, dbt, Kubernetes, SFTP) | Limitada a AWS SDK/API (HTTP externo posible, sin ecosistema de providers) | Ninguna — solo Glue |

**Conclusión práctica**: se elige **Airflow/MWAA** con ese stack ya instalado, backfills declarativos por fecha, sensores ricos o integración pesada fuera de AWS. Se elige **Step Functions** cuando el pipeline vive mayormente en AWS y no vale la pena pagar un piso fijo por un orquestador encendido 24/7. Se elige **Glue Workflows** solo si el 100% del pipeline es Glue. Este proyecto eligió Step Functions: pipeline 100% AWS, con más flexibilidad que Glue Workflows (paralelismo, `.sync`, IAM granular) sin el piso fijo de MWAA.

---

## 4. Equivalencias de orquestación en Azure y Databricks

| Concepto de Step Functions | Azure Data Factory / Synapse / Fabric | Databricks Lakeflow Jobs |
|---|---|---|
| State machine | Pipeline | Job |
| Task/State | Activity | Task |
| Retry | Política de reintentos en la activity (`retry`, `retryIntervalInSeconds`) | Configuración de reintentos por task |
| Schedule | Trigger de tipo *Schedule* (o *Tumbling window* para data intervals) | Trigger tipo *Scheduled* (cron) |
| Sync wait (`.sync`) | Ejecución secuencial de activities dentro del control flow del pipeline (hay wait explícito con `Wait activity` o dependencias `Success`) | Task `run_job` esperando a que el Job invocado termine [Databricks Docs](https://docs.databricks.com/en/jobs/index.html) |
| Choice/Branching | Activity `If Condition` / `Switch` | `if/else` visual dentro del Job [Databricks Docs](https://docs.databricks.com/en/jobs/index.html) |
| Map/Parallel | Activity `ForEach` (con `isSequential` o paralelo) | `for each` task type [Databricks Docs](https://docs.databricks.com/en/jobs/index.html) |
| Trigger por evento (archivo nuevo) | Event trigger (Storage events, en Fabric vía Data Activator/Reflex y OneLake events) [Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-runs) | File arrival trigger [Databricks Docs](https://docs.databricks.com/en/jobs/index.html) |
| Servicio administrado tipo Airflow | Managed Airflow dentro de Data Factory (aprovisiona un entorno de Airflow administrado, con un costo de entorno similar en filosofía a MWAA) | No aplica — Lakeflow Jobs es la alternativa nativa |
| Integración de sistemas heterogéneos vía conectores de bajo código | Logic Apps (conectores, más orientado a integración de aplicaciones que a ETL de datos) | No aplica directamente |

Nota sobre Azure: **Azure Data Factory está siendo sucedido por Data Factory dentro de Microsoft Fabric**, descrito por Microsoft como "la próxima generación de Azure Data Factory, con una arquitectura más simple, IA integrada y nuevas funcionalidades", recomendando a proyectos nuevos empezar directamente en Fabric [Microsoft Learn](https://learn.microsoft.com/en-us/azure/data-factory/introduction).

---

## 5. Cómputo: Glue vs EMR vs EMR Serverless vs Lambda vs ECS Fargate

| Servicio | Cuándo usarlo | Cuándo NO usarlo |
|---|---|---|
| **Glue ETL (Spark)** | ETL batch programado, integración nativa con Data Catalog, sin querer administrar clusters | Cargas interactivas de muy baja latencia, necesidad de control fino de configuración de cluster/Spark avanzada |
| **EMR** | Necesitás control total del cluster (versiones específicas de Hadoop/Spark/Hive/Presto, instalar librerías del sistema, clusters de larga duración compartidos por varios equipos) | Cargas esporádicas donde no vale la pena administrar un cluster persistente |
| **EMR Serverless** | Igual que EMR pero sin gestionar la infraestructura del cluster, con escalado automático por job | Cuando ya usás Glue y no necesitás las librerías/versión específicas que EMR ofrece por fuera de lo que Glue soporta |
| **Lambda** | Transformaciones livianas, glue-code entre servicios, procesamiento por evento (ej. redimensionar un archivo al llegar a S3), duraciones cortas | Procesamiento de datos a gran escala o jobs de más de 15 minutos (límite duro de Lambda) |
| **ECS Fargate** | Contenedores custom con dependencias específicas (librerías no soportadas por Glue/Lambda), procesos de larga duración sin servidor que administrar | Cuando Glue ya resuelve el caso de forma más simple y con menos código de infraestructura |

**Equivalentes en Azure**: Azure Databricks (el más comparable a EMR/Glue combinados), Synapse Spark pools, Fabric Spark, Azure Functions (equivalente a Lambda).

**Equivalentes en Databricks**: clusters clásicos (job clusters efímeros, más parecido a EMR en flexibilidad), serverless compute (filosofía similar a Glue), y Lakeflow Declarative Pipelines para transformaciones declarativas en vez de jobs Spark imperativos, con calidad de datos y expectativas declarativas incluidas [Databricks Docs](https://docs.databricks.com/aws/en/dlt/).

---

## 6. Glosario

- **DPU (Data Processing Unit)**: unidad de cómputo de Glue equivalente a 4 vCPU + 16 GB de RAM [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html).
- **State machine**: definición de un flujo de trabajo en Step Functions, compuesta por states.
- **Job bookmark**: mecanismo de Glue que recuerda qué datos ya se procesaron para no reprocesarlos.
- **Workgroup**: agrupación lógica de consultas en Athena para control de costo, permisos y versión de motor.
- **Execution role**: rol IAM que un servicio (Glue, Step Functions) asume para actuar en tu cuenta.
- **Assume role**: acción de tomar temporalmente los permisos de un rol IAM.
- **Least privilege**: principio de dar solo los permisos estrictamente necesarios.
- **Parameter (SSM)**: par clave-valor almacenado en Parameter Store, estándar o avanzado.
- **Log stream**: secuencia de eventos de log dentro de un log group de CloudWatch, típicamente una por ejecución.
- **Log group**: contenedor de log streams en CloudWatch Logs, generalmente uno por recurso (job, función).
- **Catálogo (Data Catalog)**: repositorio central de metadatos técnicos (bases, tablas, esquemas, particiones).
- **External table**: tabla cuyo contenido vive fuera del motor de consulta (por ejemplo, archivos en S3) y cuya metadata apunta a esa ubicación.
- **Iceberg snapshot**: versión inmutable del estado de una tabla Iceberg en un momento dado, base del time travel.
- **Job bookmark rewind**: volver el estado de un bookmark a una ejecución anterior para reprocesar desde ahí (backfill controlado).
- **Crawler**: proceso de Glue que escanea una fuente de datos y genera/actualiza tablas en el Data Catalog automáticamente.
- **Trust policy**: política JSON adjunta a un rol IAM que define quién puede asumirlo.
- **Permission policy**: política JSON que define qué puede hacer una identidad una vez autenticada.
- **Role chaining**: asumir un rol IAM desde otro rol ya asumido, limitado a sesiones de 1 hora.
- **Standard workflow**: tipo de Step Functions con ejecuciones de hasta 1 año y semántica exactly-once.
- **Express workflow**: tipo de Step Functions de hasta 5 minutos, facturado por duración y memoria.
- **Integration pattern `.sync`**: patrón de Step Functions que espera a que el job invocado termine antes de continuar.
- **Integration pattern `.waitForTaskToken`**: patrón de Step Functions que espera un callback externo con un token.
- **ASL (Amazon States Language)**: lenguaje JSON declarativo para definir máquinas de estado en Step Functions.
- **JSONata**: lenguaje de consulta/transformación de JSON que Step Functions soporta como alternativa a los payload templates clásicos.
- **Worker type (G.1X/G.2X)**: perfil de hardware asignado a cada worker de un job Spark de Glue.
- **Job bookmark context (transformation_ctx)**: identificador que vincula un nodo del script con su estado de bookmark persistido.
- **Auto scaling (Glue)**: ajuste dinámico del número de workers de un job Spark según demanda real, disponible desde Glue 3.0+.
- **Flex execution**: clase de ejecución de Glue de menor prioridad/costo para jobs no urgentes.
- **Hierarchical namespace**: estructura de carpetas reales que ADLS Gen2 agrega sobre Blob Storage.
- **Unity Catalog**: capa de gobierno de datos e IA de Databricks con namespace de tres niveles.
- **Secret scope**: colección nombrada de secretos en Databricks.
- **SQL Warehouse**: recurso de cómputo de Databricks optimizado para consultas SQL, con escalado serverless.
- **Lakeflow Declarative Pipelines**: framework declarativo de Databricks (ex Delta Live Tables) para pipelines batch/streaming.
- **`run_job` task**: tipo de task de Databricks Jobs que invoca y espera la finalización de otro Job.
- **Trino**: motor de consultas SQL open source en el que se basa el motor v3 de Athena.

---

## 7. Preguntas de entrevista

1. **¿Por qué Glue Workflows no alcanza como orquestador general?**
   Porque solo encadena jobs y crawlers de Glue; no integra nativamente con Lambda, ECS, SageMaker ni nada fuera de Glue, a diferencia de Step Functions [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/orchestrate-using-workflows.html).

2. **¿Qué diferencia a un workflow Standard de uno Express en Step Functions?**
   Standard corre hasta 1 año, exactly-once, facturado por transición de estado; Express corre hasta 5 minutos, at-least-once/at-most-once, facturado por ejecuciones, duración y memoria [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-standard-vs-express.html).

3. **¿Qué hace el integration pattern `.sync` y por qué se usa para encadenar jobs de Glue?**
   Hace que Step Functions espere a que el job invocado (`glue:startJobRun.sync`) termine antes de avanzar, en vez de solo confirmar que arrancó [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html).

4. **¿Cuándo elegirías MWAA por sobre Step Functions?**
   Con DAGs versionados en Python testeables, backfills declarativos por fecha, sensores ricos, o integración pesada fuera de AWS — a cambio de un costo fijo de entorno [AWS MWAA Pricing](https://aws.amazon.com/managed-workflows-for-apache-airflow/pricing/).

5. **¿Qué es un job bookmark en Glue y qué limitación tiene con Python shell jobs?**
   Guarda qué datos ya se procesaron para no reprocesarlos; los jobs Python shell no lo soportan, solo los Spark con ciertas fuentes [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html).

6. **¿Diferencia entre un worker G.1X y uno G.2X en Glue?**
   G.1X = 1 DPU (4 vCPU, 16 GB RAM); G.2X = 2 DPU (8 vCPU, 32 GB RAM) — se elige según si el cuello de botella es memoria o volumen de datos [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html).

7. **¿Por qué Athena cobra por TB escaneado y qué implica para el diseño de tablas?**
   Porque lee directamente los archivos en S3 sin warehouse precomputado; conviene particionar y usar formatos columnares comprimidos (Parquet, Iceberg) [AWS Athena Pricing](https://aws.amazon.com/athena/pricing/).

8. **¿Cuándo usarías SSM Parameter Store en vez de Secrets Manager?**
   Para configuración no sensible o no rotativa (bucket, flags, rutas), evitando el costo por secreto y por llamada de Secrets Manager; Parameter Store estándar es gratis [AWS Systems Manager Pricing](https://aws.amazon.com/systems-manager/pricing/) [AWS Secrets Manager Pricing](https://aws.amazon.com/secrets-manager/pricing/).

9. **¿Qué es Unity Catalog y en qué se diferencia de Glue Data Catalog?**
   Capa de gobierno de Databricks con namespace de tres niveles (`catalog.schema.objeto`) que integra control de acceso, linaje y auditoría, mientras Glue Data Catalog es un metastore técnico (Hive Metastore-compatible) que delega el control fino a Lake Formation [Databricks Docs](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) [AWS Glue Docs](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html).

10. **¿Qué rol IAM mínimo necesita una máquina de estados para arrancar un job de Glue con `.sync`?**
    `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns` y `glue:BatchStopJobRun` sobre el recurso del job [AWS Step Functions Docs](https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html).

# Gap 3 — AWS a costo 0 más allá de 6 meses: verificación con fuentes oficiales

Fecha de verificación: 2026-09-04. Nota metodológica: el buscador web (WebSearch) se agotó su cupo de sesión antes de poder rastrear foros/re:Post/blogs de forma independiente; todo lo que sigue proviene de **WebFetch directo sobre páginas oficiales de docs.aws.amazon.com y aws.amazon.com**. Donde no encontré el dato en fuente primaria lo marco explícitamente como **NO VERIFICADO**.

## 1. Qué pasa al terminar el Free account plan (el hallazgo más importante)

Cita textual de la documentación oficial ([docs.aws.amazon.com/awsaccountbilling/.../free-tier-plans.html](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html)):

> "After your free account plan expires, your account closes automatically, and you lose access to your resources and data. AWS retains your content for 90 days before permanently deleting your account and all associated resources. To maintain your account access, you can upgrade to a Paid account plan with pay-as-you-go pricing within 90 days."

Y en la tabla comparativa de la misma página:

> "Account closes when credits are depleted or when the plan duration ends" (Free plan) vs. "Account doesn't close when credits are depleted" (Paid plan).

La FAQ oficial ([aws.amazon.com/free/free-tier-faqs/](https://aws.amazon.com/free/free-tier-faqs/)) confirma lo mismo casi palabra por palabra: *"When your free plan expires, AWS closes your account, and you'll lose access to your resources and data. AWS will retain your data for 90 days after your free plan expires... If you don't upgrade your account within 90 days, AWS will permanently erase your AWS account and all its content."*

**Conclusión determinante:** en el modelo "Free account plan" (vigente desde el 15 de julio de 2025, confirmado en [aws.amazon.com/s3/pricing/](https://aws.amazon.com/s3/pricing/): *"As of July 15, 2025, new AWS customers will receive up to $200 in AWS Free Tier credits..."*), **no existe un modo "solo Always Free" indefinido dentro de esa cuenta**. A los 6 meses (o antes si se agota el crédito), la cuenta entera se cierra automáticamente, con 90 días de gracia para reactivarla pasando a Paid plan, y borrado permanente después. Esto es distinto del modelo pre-julio-2025, donde una cuenta vieja podía seguir usando Always Free tier sin límite de tiempo aunque el 12-months-free expirara.

## 2. Servicios excluidos del Free account plan

Ninguna de las páginas oficiales fetcheadas (`free-tier.html`, `free-tier-plans.html`, `free-tier-faqs`) publica una lista nominal completa. El texto oficial es genérico:

> "Free account plans don't include access to AWS services and features that could possibly deplete your credits, or hardware purchases. Some service examples include Savings Plans, Reserved Instances, and certain AWS Marketplace offers that can incur charges."

La página aws.amazon.com/free/ (renderizada por JS, por lo que WebFetch solo capturó el HTML estático) solo permitió confirmar textualmente que EC2, S3, Aurora, RDS, DynamoDB, SageMaker AI y Bedrock **aparecen listados como disponibles** en el filtro de servicios del Free Tier, y que "Bedrock AgentCore" aparece marcado como exclusivo de Paid plan. **No pude confirmar ni descartar** con fuente oficial el estado de Glue, Athena, Kinesis, Step Functions, EventBridge, ECR/ECS Fargate, EMR Serverless, MWAA, CloudFormation ni IAM en el Free account plan — la doc oficial remite circularmente a la misma página filtrable que no expone el detalle en texto plano, y no pude usar WebSearch para cruzar con re:Post/reddit por agotamiento de cupo en esta sesión. **Marcado explícitamente como NO VERIFICADO.**

## 3. Tabla de free tier por servicio

| Servicio | Tipo de free tier | Límite exacto citado | Free account plan | URL |
|---|---|---|---|---|
| S3 | NO VERIFICADO (Always Free vs 12m) | No se encontró cifra exacta en fuente oficial en esta sesión (páginas de pricing/docs no la mostraron) | Sí, listado como disponible | [s3/pricing](https://aws.amazon.com/s3/pricing/) |
| Lambda | Always Free (histórico, redacción actual no lo etiqueta explícitamente) | "one million requests and 400,000 GB-seconds per month" | No verificado | [lambda/pricing](https://aws.amazon.com/lambda/pricing/) |
| DynamoDB | Always Free | "25 WCUs, 25 RCUs", "25 GB of data storage", "2.5 million stream read requests", "1 GB data transfer out (15 GB primeros 12 meses)" | Sí, listado como disponible | [dynamodb/pricing](https://aws.amazon.com/dynamodb/pricing/) |
| Step Functions | Always Free confirmado explícitamente | "4,000 free state transitions per month... does not automatically expire at the end of your 12 month AWS Free Tier term, and is available to both existing and new AWS customers indefinitely" | No verificado | [step-functions/pricing](https://aws.amazon.com/step-functions/pricing/) |
| CloudWatch | Always Free ("permanently free tier benefits") | "10 Metrics", "1 Million API requests", "10 Alarm metrics", "3 Custom Dashboards", "5 GB Data" (logs), "1,800 min" Live Tail | No verificado | [cloudwatch/pricing](https://aws.amazon.com/cloudwatch/pricing/) |
| EventBridge | Always Free (parcial) | Scheduler: "14,000,000 invocations per month"; Schema Registry discovery: "5 million ingested events per month"; eventos de servicio AWS ingeridos gratis | No verificado | [eventbridge/pricing](https://aws.amazon.com/eventbridge/pricing/) |
| SQS | Always Free | "All customers can make 1 million Amazon SQS requests for free each month" | No verificado | [sqs/pricing](https://aws.amazon.com/sqs/pricing/) |
| SNS | No verificado el límite exacto ni el rótulo | Página no mostró cifra específica en el fetch | No verificado | [sns/pricing](https://aws.amazon.com/sns/pricing/) |
| Glue Data Catalog | Always Free (implícito, no rotulado explícitamente) | "The first million objects stored are free"; "the first million accesses are free" (mensual) | No verificado | [glue/pricing](https://aws.amazon.com/glue/pricing/) |
| Glue ETL (jobs/crawlers) | **Ninguno** | "No free tier is explicitly mentioned"; $0.44/DPU-hora | No verificado | [glue/pricing](https://aws.amazon.com/glue/pricing/) |
| Athena | **Ninguno** | Sin mención de free tier; $5 por TB escaneado | No verificado | [athena/pricing](https://aws.amazon.com/athena/pricing/) |
| Kinesis, ECR/ECS Fargate, EC2, RDS, MWAA, EMR Serverless, SageMaker, IAM, CloudFormation | No verificado en esta sesión | — | No verificado (salvo EC2/RDS/SageMaker listados en la página de filtro como "disponibles", sin certeza del alcance) | [docs free-tier.html](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier.html) |

## 4. Costo real de un pipeline modesto (Athena + Glue)

Sin free tier en ninguno de los dos:
- Athena: 10 GB escaneados/mes × $5/TB = 10/1024 TB × $5 ≈ **$0.05/mes** (el escaneo es tan chico que es casi irrelevante en un demo real; el riesgo es escanear de más por falta de particionado).
- Glue ETL: 2 jobs × 10 min (0.1667 h) × 2 DPU × $0.44/DPU-h = 2 × 0.1667 × 2 × 0.44 ≈ **$0.29/mes** (más el mínimo de facturación por job, que AWS redondea a 1 minuto mínimo, no a 10; el cálculo de arriba ya usa 10 min completos).
- Total combinado Athena + Glue ETL en este escenario modesto: **~$0.35 USD/mes**, no cero, pero bajo — el riesgo real está en escalar el escaneo (datasets sin particionar/comprimir) o correr crawlers repetidamente, ahí sube rápido.

## 5. Alternativa serverless de costo casi cero

No pude verificar con fuente primaria "S3 + Lambda + DuckDB embebido en Lambda + Athena mínimo" como patrón oficialmente respaldado por AWS (es un patrón de la comunidad, no documentado por AWS; **NO VERIFICADO** en esta sesión por falta de cupo de búsqueda). Lo que sí es verificable con fuente oficial:
- Un sitio estático en S3 (hosting de website) cae dentro del free tier de S3, pero **no confirmé si sigue siendo Always Free o 12 meses** en el modelo nuevo (ver tabla).
- Lambda, SQS, Step Functions, EventBridge Scheduler y CloudWatch tienen cuotas Always Free confirmadas arriba que alcanzan cómodamente para un demo de bajo tráfico (miles de invocaciones/mes).
- Athena y Glue ETL no tienen free tier pero, en volúmenes de portfolio (GB, no TB), el costo real es de centavos, como se calculó en la sección 4.
- Estimación mensual esperada para un demo público de portfolio con tráfico bajo, una vez pasado el free account plan / los 6 meses de crédito, con el pipeline completo (S3 + Lambda + Glue Data Catalog + Athena + Step Functions + CloudWatch, sin RDS/EC2/EMR/SageMaker permanentes): **entre $1 y $5 USD/mes** es un rango razonable basado en las cuotas Always Free confirmadas y el cálculo de Athena/Glue de arriba, pero esta cifra es una estimación mía, no una cita oficial.

## Veredicto

**¿Es viable dejar el proyecto corriendo en AWS a costo $0 exacto más allá de 6 meses?** **No**, si "en AWS" significa "en la cuenta actual del Free account plan": la documentación oficial confirma que esa cuenta se **cierra automáticamente** a los 6 meses o al agotar el crédito, con 90 días de gracia y borrado definitivo después — no hay un modo "solo Always Free" sostenido indefinidamente dentro de un Free account plan.

Sí es viable, en cambio, si el plan es: (a) usar los 6 meses de Free account plan para desarrollar y demostrar el proyecto completo, o (b) subir a Paid account plan (que no cierra la cuenta) y operar dentro de las cuotas Always Free confirmadas — Lambda, DynamoDB, Step Functions, SQS, CloudWatch, EventBridge, Glue Data Catalog — evitando Athena/Glue ETL salvo volúmenes mínimos (costo real ~$0.35/mes en el escenario modesto calculado). El subconjunto de servicios con cuota Always Free verificada en fuente oficial que soporta esto es: **Lambda, DynamoDB, Step Functions, SQS, CloudWatch, EventBridge (Scheduler/Schema Registry), Glue Data Catalog**. S3, SNS, y el resto de servicios (Kinesis, EC2, RDS, MWAA, EMR Serverless, SageMaker, ECR/ECS Fargate) quedan **NO VERIFICADOS** con fuente oficial en esta sesión y deberían confirmarse antes de comprometer la arquitectura del portfolio a ellos.

**Estimación realista:** $1–$5 USD/mes en Paid account plan operando dentro de cuotas Always Free más Athena/Glue ETL de bajo volumen — no es $0 estricto, pero es sostenible indefinidamente sin sorpresas de facturación grandes, a diferencia de dejarlo en Free account plan (que se autodestruye a los 6 meses).

## Fuentes consultadas
- https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier.html
- https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html
- https://aws.amazon.com/free/free-tier-faqs/
- https://aws.amazon.com/free/
- https://aws.amazon.com/s3/pricing/
- https://aws.amazon.com/lambda/pricing/
- https://aws.amazon.com/dynamodb/pricing/
- https://aws.amazon.com/step-functions/pricing/
- https://aws.amazon.com/athena/pricing/
- https://aws.amazon.com/glue/pricing/
- https://aws.amazon.com/cloudwatch/pricing/
- https://aws.amazon.com/eventbridge/pricing/
- https://aws.amazon.com/sqs/pricing/
- https://aws.amazon.com/sns/pricing/

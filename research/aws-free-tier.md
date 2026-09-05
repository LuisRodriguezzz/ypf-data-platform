# AWS Free Tier 2026 y emulación local — lectura profunda de fuentes primarias

Fecha de la investigación: 2026-09-04. Este informe profundiza (lectura completa, no solo snippets) sobre las 8 fuentes designadas para el ángulo "AWS Free Tier / emulación local" del proyecto portfolio YPF Data Platform, corrige matices del barrido previo y cierra parcialmente las preguntas abiertas dentro del presupuesto de búsqueda disponible en esta sesión (el cupo de `WebSearch` se agotó a mitad de la ronda; se compensó con `WebFetch` directo sobre URLs oficiales conocidas).

## 1. El modelo Free Plan / Paid Plan (post 15-jul-2025) — confirmado con matices

La lectura completa de `docs.aws.amazon.com/.../free-tier.html` confirma la estructura general pero **corrige un detalle del barrido previo**: la documentación oficial dice literalmente que "**si eres nuevo en AWS, recibes USD 100 en créditos al crear una cuenta, sin importar tu plan de cuenta**" ("regardless of your account plan") y que puedes ganar "hasta un adicional de USD 100 en créditos completando actividades". Es decir, los USD 100 iniciales no están condicionados a elegir el Free account plan — se otorgan también en el Paid account plan. El diferencial entre planes no es el monto de crédito sino **el acceso a ofertas**: el Free account plan solo tiene activas ofertas "Always Free"; el Paid account plan puede tener activas tanto "Always Free" como "Short-term trials".

Cita textual clave: *"Your free account plan ends after six months or when your credits are fully used – whichever occurs first."* Y sobre la razón de las exclusiones: *"free account plans don't have access to certain AWS services that would rapidly consume the entire AWS Free Tier credit amount, or hardware purchases."*

El anuncio original (`aws.amazon.com/about-aws/whats-new/2025/07/...`) fue publicado, según el propio contenido leído, el **16 de julio de 2025** (no el 15 como decía el barrido previo — diferencia menor de un día que vale la pena anotar si se cita la fecha exacta en el proyecto). Cita textual: *"$100 in AWS credits upon sign-up and can earn an additional $100 in credits by using services such as Amazon EC2 and Amazon Bedrock"*, con vigencia *"6 months after sign-up or when Free Tier credits are depleted, whichever comes first"*, cobertura de *"over 200 services"*, disponible *"in all AWS Regions except AWS GovCloud (US) and China Regions"*, y posibilidad de *"easily upgrade to the paid plan with a single click"*.

**Pregunta abierta que sigue sin cerrarse del todo:** ni la FAQ oficial (`aws.amazon.com/free/free-tier-faqs/`) ni la página `aws.amazon.com/free/` devolvieron, en esta ronda de lectura vía WebFetch, el listado línea por línea de los servicios excluidos del Free account plan ni las 5 actividades exactas para ganar los USD 100 adicionales — ambas páginas remiten circularmente una a la otra ("visit AWS Free Tier" / "ver AWS Free Tier FAQs") y a un widget interactivo ("Explore AWS widget") dentro de la consola de facturación, que no es accesible sin sesión autenticada. Lo único confirmado con cita textual sobre las actividades es la mención genérica a "usar servicios como Amazon EC2 y Amazon Bedrock"; no se logró extraer una lista cerrada de 5 ítems (el barrido previo mencionaba EC2, RDS, Lambda, Bedrock y AWS Budgets, pero esta ronda no pudo confirmarlo palabra por palabra en la fuente oficial — se recomienda tratarlo como no verificado hasta revisarlo manualmente dentro de la consola).

**Recomendación práctica para el proyecto:** dado que el crédito de $200 es temporal (6 meses) y ya no hay period clásico de 12 meses para cuentas nuevas, el diseño del proyecto YPF debe apoyarse primariamente en los servicios **"Always Free"** (permanentes) y minimizar dependencia de servicios que solo tienen trial o crédito temporal, para que el portfolio siga funcionando sin costo después de que expire la ventana de 6 meses.

## 2. Servicios clave verificados — tabla consolidada

| Servicio | Tipo de free tier | Límite exacto (cita) | Fuente |
|---|---|---|---|
| AWS Glue Data Catalog | Always Free (aparente, sin mención de expiración) | "The first million objects stored are free, and the first million accesses are free" | glue/pricing/ |
| Glue Crawlers | Sin free tier | $0.44/DPU-hora, facturado por segundo | glue/pricing/ |
| Glue ETL jobs / Interactive Sessions | Sin free tier | $0.44/DPU-hora; ejemplo: job de 15 min con 6 DPU = $0.66 | glue/pricing/ |
| **Glue Data Quality (DQDU)** | Sin free tier explícito | $0.44/DPU-hora para recomendación/evaluación (mínimo 2 DPU, 1 min); detección de anomalías: "1 DPU por statistic" | glue/pricing/ |
| AWS Step Functions | **Always Free confirmado explícitamente** | *"does not automatically expire at the end of your 12 month AWS Free Tier term, and is available to both existing and new AWS customers indefinitely"* — 4.000 transiciones/mes; $0.000025/transición adicional en us-east-1 | step-functions/pricing/ |
| AWS CloudFormation | Parece Always Free (sin mención de expiración) | **1.000 handler operations/mes** gratis; una "handler operation" = acciones CREATE/UPDATE/DELETE/READ/LIST sobre un resource type, o CREATE/UPDATE/DELETE sobre un Hook type; primeros 30s de duración sin cargo adicional por operación; excedente $0.0009/operación + $0.00008/segundo adicional. Aplica solo a proveedores de recursos de terceros (no `AWS::`/`Alexa::`) y hooks personalizados — los recursos AWS estándar no generan cargo de CloudFormation | cloudformation/pricing/ |
| Redshift Serverless | Trial temporal (no always free) | **$300 en créditos, 90 días desde el registro**, aplicable a "compute and usage"; solo para cuentas que nunca usaron Redshift Serverless; la página consultada está fechada 2026 y presenta la oferta como activa, sin fecha de vencimiento del programa en sí | redshift/free-trial/ |
| Amazon S3 | 12 meses (no confirmado si varía por región) | 5GB, 20K GET, 2K PUT — no se encontró texto que documente diferencias regionales explícitas en la página de pricing consultada | s3/pricing/ |

## 3. LocalStack: confirmación exacta de exclusiones en Hobby

La lectura completa de `localstack.cloud/pricing` confirma con precisión el hallazgo previo: el plan **Hobby** (gratuito, uso no comercial) cubre "30+ emulated services" con "1 personal sandbox" y "Run tests in CI", pero **Kinesis Streams/Firehose/Data Analytics, AWS Glue y Amazon Athena aparecen marcados explícitamente como no disponibles (✗) en Hobby**, disponibles recién en Base y Ultimate. Precios confirmados: **Base USD 39/mes (facturación anual) o USD 45/mes**, con 55+ servicios, persistencia de estado local y enforcement de políticas IAM; **Ultimate USD 89/mes (anual)**, 110+ servicios, AWS Replicator y soporte prioritario; Enterprise requiere contacto comercial. Esto confirma que para emular un pipeline con Glue+Athena de forma gratuita, LocalStack Hobby **no sirve** — hay que usar AWS real (dentro de los límites always-free) o herramientas alternativas (DuckDB/Athena local vía Trino, Moto solo para S3/DynamoDB/SQS/Lambda, etc.).

## 4. Moto: no se obtuvo lista textual completa, pero se confirma el mecanismo

La página del repositorio de GitHub no expone en su README la lista completa de servicios soportados; remite al archivo `IMPLEMENTATION_COVERAGE.md` del propio repositorio como fuente autoritativa. Se confirma la licencia **Apache-2.0** y el patrón de instalación granular, p. ej. `pip install 'moto[ec2,s3,all]'` (variante del comando reportado en el barrido previo). Para efectos del proyecto, sigue siendo válida la recomendación de usar Moto para pruebas unitarias de la capa S3/DynamoDB/SQS/Lambda sin necesidad de contenedor, y reservar AWS real (always-free) para todo lo relacionado con Glue Data Catalog/Athena, que ni Moto ni LocalStack Hobby cubren de forma confiable y gratuita.

## 5. Preguntas abiertas: estado de cierre en esta ronda

| Pregunta | Estado | Detalle |
|---|---|---|
| Lista exacta de exclusiones del Free account plan | **No cerrada** | La FAQ oficial remite circularmente a la página `/free/` y a un widget interno de consola no accesible por WebFetch; solo se confirma la razón general (servicios que consumen rápido el crédito o requieren hardware) |
| Glue Data Quality (DQDU): free tier y tarifa | **Cerrada parcialmente** | Confirmado: $0.44/DPU-hora (recomendación/evaluación, mínimo 2 DPU), sin mención de free tier propio; anomaly detection cobra "1 DPU por statistic" |
| Límite exacto de CloudFormation free tier | **Cerrada** | 1.000 handler operations/mes, definición exacta de "handler operation" confirmada, con tarifas de excedente ($0.0009/op + $0.00008/seg tras 30s) |
| Vigencia de Redshift Serverless trial en 2026 | **Cerrada (con matiz)** | $300/90 días confirmado activo en una página fechada 2026; no se halló fecha de expiración del programa en sí (podría discontinuarse sin previo aviso, como toda oferta de trial) |
| Diferencias regionales de free tier (S3, CloudWatch) fuera de us-east-1 | **No cerrada** | La página de pricing de S3 no aborda el tema de free tier regional (solo trata Replication Time Control, un feature distinto); no se pudo verificar con una segunda fuente por agotamiento del cupo de WebSearch de la sesión |
| SageMaker Studio Lab en 2026: costo y límites | **No cerrada** | La URL oficial `aws.amazon.com/sagemaker/studio-lab/` redirige a `studiolab.sagemaker.aws`, que devolvió HTTP 403 Forbidden al fetch automatizado; no se pudo confirmar límites de cupo de CPU/GPU en esta ronda. Se recomienda verificación manual en navegador antes de incluirlo como dependencia del diseño |

## 6. Implicancias de diseño para el proyecto YPF Data Platform

1. **Base de cómputo serverless "siempre gratis":** Lambda (1M req + 400K GB-s), DynamoDB (25GB + 25 WCU/RCU provisionados), SQS/SNS (1M req c/u), Step Functions (4.000 transiciones, confirmado explícitamente como indefinido), CloudWatch (10 métricas, 10 alarmas, 5GB logs) y ahora también **Glue Data Catalog (1M objetos + 1M solicitudes)** y **CloudFormation (1.000 handler operations)** son la columna vertebral segura del diseño, sin riesgo de expiración a los 6 o 12 meses.
2. **Athena y Glue ETL/Crawlers/Data Quality son la variable de costo real:** todos cobran desde el primer uso ($5/TB escaneado en Athena; $0.44/DPU-hora en Glue para cualquier variante de cómputo, incluida Data Quality). El diseño debe particionar agresivamente los datos de YPF (por fecha/yacimiento) para minimizar bytes escaneados, y preferir Glue Data Catalog + Athena sobre Glue ETL jobs cuando sea posible, ya que solo el catálogo tiene tier gratis.
3. **LocalStack Hobby no es viable para emular Glue/Athena/Kinesis** — confirmado explícitamente por la matriz de pricing. Para desarrollo/CI local del proyecto conviene: Moto para S3/DynamoDB/SQS/Lambda, y para Glue/Athena usar directamente la cuenta AWS real dentro de los límites always-free (o herramientas open-source equivalentes como DuckDB+Iceberg/Trino localmente, fuera del alcance de este documento).
4. **No depender de créditos temporales para el "estado estable" del portfolio:** Redshift Serverless ($300/90 días) y el crédito general de $200/6 meses no deben ser parte de la arquitectura permanente del proyecto — solo usarlos para picos de prueba puntuales, documentando que tras 90 días (Redshift) o 6 meses (crédito general) el costo pasa a ser pay-as-you-go real.
5. **CloudFormation es gratis para IaC estándar:** como los recursos `AWS::*` no generan cargo de CloudFormation y el free tier de 1.000 handler operations solo aplica a recursos de terceros/custom hooks, usar Terraform o CloudFormation nativo para desplegar la infraestructura del proyecto (S3, Lambda, Glue Catalog, Step Functions, DynamoDB) no añade costo por el motor de IaC en sí — el riesgo de costo sigue estando en los recursos subyacentes (NAT Gateway, Elastic IPs huérfanas, Athena mal particionado), como ya identificó el barrido previo.
6. **SageMaker Studio Lab queda como riesgo documentado, no como dependencia crítica:** al no poder confirmarse su estado operativo/límites en esta ronda (403 Forbidden en el fetch), si el proyecto planea usarlo para notebooks de análisis exploratorio de datos de YPF, se recomienda una verificación manual previa en navegador antes de comprometerlo en el diseño de arquitectura.

## 7. Correcciones al barrido previo

- La fecha del anuncio original del nuevo modelo de Free Tier es **16 de julio de 2025**, no 15 de julio (diferencia de un día, detectada al leer el contenido completo del anuncio).
- El crédito inicial de USD 100 **no depende de elegir el Free account plan**: se otorga "regardless of your account plan" (incluso en Paid account plan); lo que sí depende del plan elegido es el acceso a ofertas Always Free vs. Always Free + Short-term trials.
- No se pudo confirmar (ni refutar) la lista exacta de "5 actividades" para ganar los USD 100 adicionales que mencionaba el barrido previo (EC2, RDS, Lambda, Bedrock, Budgets); la única cita textual disponible en las fuentes oficiales leídas menciona genéricamente "servicios como Amazon EC2 y Amazon Bedrock". Se recomienda tratar esa lista de 5 ítems como no verificada hasta revisión manual en consola.
- Se confirma con mayor precisión el hallazgo sobre Step Functions: la fuente usa lenguaje explícito de permanencia ("does not automatically expire... indefinitely"), reforzando la clasificación "always free" con más fuerza que una simple inferencia de página de marketing.
- Nuevo dato incorporado (no estaba en el barrido previo): límite exacto y definición de CloudFormation free tier (1.000 handler operations/mes, con distinción clara entre recursos AWS nativos —gratis siempre a nivel de motor CloudFormation— y recursos de terceros/hooks personalizados).

## 8. Limitaciones de esta ronda de investigación

El cupo de `WebSearch` de la sesión se agotó (200/200) antes de poder completar búsquedas adicionales para cerrar las preguntas sobre diferencias regionales de free tier y el estado exacto de SageMaker Studio Lab en 2026; ambos puntos quedaron parcialmente abiertos y se listan explícitamente arriba en vez de rellenarse con inferencia. Todas las demás fuentes designadas fueron leídas de forma completa vía `WebFetch` directo.

## Fuentes

1. https://docs.aws.amazon.com/en_us/awsaccountbilling/latest/aboutv2/free-tier.html
2. https://aws.amazon.com/free/free-tier-faqs/
3. https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan/
4. https://aws.amazon.com/free/
5. https://aws.amazon.com/glue/pricing/
6. https://aws.amazon.com/step-functions/pricing/
7. https://www.localstack.cloud/pricing
8. https://github.com/getmoto/moto
9. https://aws.amazon.com/cloudformation/pricing/
10. https://aws.amazon.com/redshift/free-trial/
11. https://aws.amazon.com/sagemaker/studio-lab/ (redirige a https://studiolab.sagemaker.aws/, HTTP 403 al fetch automatizado — no verificado en esta ronda)
12. https://aws.amazon.com/s3/pricing/

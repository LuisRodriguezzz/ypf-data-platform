# Casos de ML en O&G y arquitecturas de referencia — lectura profunda

*Investigación para el diseño de un proyecto portfolio de Data Engineering end-to-end sobre YPF, ejecutable en nubes gratuitas (AWS Free Tier, Databricks Free Edition) o en local distribuido. Fecha de cierre: 2026-09-04.*

> **Nota metodológica**: durante esta ronda se agotó el presupuesto de `WebSearch` de la sesión (200/200 llamadas) antes de poder ejecutar las 5 búsquedas adicionales planeadas para cerrar las preguntas abiertas sobre benchmarks de DCA, type curves propias de YPF y madurez de la OSDU community edition. Esas preguntas quedan documentadas como abiertas al final. Sí se pudo hacer `WebFetch` completo de las 8 fuentes prioritarias listadas en el brief (una, el PDF de arXiv 2504.13976, llegó corrupta/no legible en texto — ver sección correspondiente).

## 1. YPF y el Real Time Intelligence Center (RTIC): lo que la fuente realmente dice

La lectura completa del artículo de lmneuquen.com/mase aporta precisiones que **corrigen** una cifra del barrido previo. El hallazgo anterior afirmaba que el RTIC monitorea "más de 2.000 pozos". La lectura del cuerpo del artículo, sin embargo, arroja una cifra distinta y más verificable en el texto extraído:

- El centro está ubicado en el **piso 26 de la torre de YPF en Puerto Madero**, Buenos Aires, operando pozos a **1.400 km de distancia** (Vaca Muerta, Neuquén).
- El texto extraído habla de **"20 equipos de torre" (rigs) operando en Vaca Muerta** monitoreados desde el centro, no de 2.000 pozos individuales. Es posible que la cifra "+2.000 pozos" del barrido previo corresponda a otro pasaje del artículo (el stock histórico de pozos perforados por la operadora) y la cifra "20 equipos" a la operación en tiempo real actual; el texto disponible no permite conciliar ambas cifras con certeza, por lo que **se recomienda tratar "2.000 pozos monitoreados en tiempo real" como no confirmado** y usar en su lugar la cifra verificada de **20 equipos de perforación activos** bajo supervisión remota, salvo relectura manual del artículo original que concilie ambos números.
- Starlink permite procesar **"hasta 35 millones de datos"** (la unidad exacta — registros, puntos de sensor, mensajes — no quedó especificada en el extracto).
- Se monitorean **más de 60 variables en tiempo real** por pozo/equipo.
- El modelo predictivo de YPF está entrenado con datos de **más de mil pozos perforados** por la operadora — esto **sí confirma** el finding previo.
- Existe un asistente conversacional interno tipo "ChatGPT" que usa el historial de pozos para apoyar decisiones operativas, pero el artículo no detalla su arquitectura técnica (LLM base, si es RAG sobre datos propios, proveedor cloud).
- Meta de eficiencia declarada: mejora de **15% a 30% hacia 2025** en operaciones de Vaca Muerta.

**Implicación para el proyecto de portfolio**: no hay una publicación técnica propia de YPF (paper, whitepaper, ingeniería de datos documentada) que describa la arquitectura del RTIC más allá de la cobertura periodística — esta pregunta abierta del barrido previo **sigue sin resolverse** tras la lectura completa; no se encontró un blog de ingeniería o paper corporativo de YPF citado por la prensa. El proyecto portfolio deberá, por lo tanto, **inspirarse** en el caso de uso (monitoreo remoto multi-pozo, +60 variables/pozo en tiempo real, modelo predictivo entrenado con histórico de miles de pozos, asistente conversacional sobre datos operativos) sin poder anclarse a especificaciones técnicas reales de YPF — hay que diseñarlo como un análogo razonable, dejando explícito en la documentación del proyecto que la arquitectura de YPF es opaca y que el diseño es una reconstrucción de buenas prácticas de industria, no una réplica.

## 2. Estándares de la industria: OSDU, PPDM, Energistics

### 2.1 OSDU Forum (The Open Group)

La lectura completa de opengroup.org/osdu-forum confirma y **actualiza ligeramente** la cifra de membresía del barrido previo (185+ → **190 organizaciones miembro**), y aporta el desglose:

| Categoría de miembro | Cifra verificada |
|---|---|
| Organizaciones miembro totales | 190 |
| Grandes operadoras petroleras desarrollando sobre OSDU | 16 |
| Proveedores de nube mayores | 4 (AWS, Google Cloud, IBM, Microsoft) |
| Proveedores/instituciones académicas | 170 |

Punto clave para el diseño del proyecto: **existe una implementación de referencia open source**, alojada en **`https://community.opengroup.org/osdu`** (GitLab), y el estándar se declara desplegable en **"Microsoft Azure, Amazon Web Services, Google Cloud Platform, y IBM Red Hat OpenShift"**. La fuente menciona releases recientes — "OSDU R3 Milestone 26" y una versión formal "OSDU Data Platform Standard, Version 1.0" — que sugieren que el estándar sigue en evolución activa. No se pudo, por agotamiento del presupuesto de búsqueda, verificar directamente en GitLab la madurez práctica de instalación local de esa implementación de referencia (issues abiertos, tiempo de setup, requisitos de infraestructura) — **pregunta abierta que persiste**.

**Relevancia para el proyecto portfolio**: dado que la implementación completa de OSDU (microservicios Java/Spring sobre Kubernetes, requiere Azure/AWS/GCP/OpenShift real o clusters K8s locales pesados) excede ampliamente el presupuesto de un Free Tier o de una laptop, la recomendación es **no desplegar OSDU real**, sino **modelar el esquema de datos conceptual de OSDU** (Well, Wellbore, WellLog, Trajectory, etc., del OSDU Data Model) como inspiración para el diseño de entidades en la capa silver/gold del lakehouse, citando el estándar como referencia de diseño más que como componente ejecutable.

### 2.2 Azure Data Manager for Energy (ADME)

La documentación oficial de Microsoft Learn (última actualización de contenido: 2025-03-23) confirma:

- ADME es un servicio **PaaS totalmente gestionado**, desarrollado **en colaboración con SLB**, que ofrece "compatibility with evolving community standards like OSDU®".
- Microsoft "offers seamless upgrades to the latest OSDU® milestone versions after testing and validation" — es decir, gestiona las actualizaciones de milestone por el cliente.
- Seguridad: cifrado en tránsito y en reposo, autenticación/autorización vía **Microsoft Entra ID**.
- Soporta **múltiples particiones de datos por instancia** ("multiple data partitions for every platform instance"), y estas se pueden crear después del despliegue inicial.
- Integración nativa con el ecosistema Microsoft: **SharePoint** (ingesta), **Synapse** (transformación/pipelines), **Power BI** (visualización, con conector ya liberado), y compatibilidad de aplicaciones **Petrel** (SLB) "out-of-the-box".
- El documento no especifica el release M26 explícitamente en el cuerpo extraído (a diferencia del hallazgo previo), aunque sí confirma que Microsoft valida y promueve versiones de milestone continuamente.

### 2.3 AWS Energy Data Insights (EDI)

El anuncio oficial (1 de mayo de 2025) confirma:

- EDI es una "plataforma de gestión de datos de subsuperficie que opera conforme al estándar OSDU®".
- El soporte gestionado se entrega vía **AWS Managed Service (AMS)**, cubriendo gestión de incidentes y backup/restore.
- Beneficio cuantificado: acelera la ingesta de datos **"de semanas a horas"**.
- Regiones confirmadas: **US East (N. Virginia), US West (Oregon), Asia Pacific (Singapore, Sydney), Europe (Ireland, Paris), South America (São Paulo)** — siete regiones, consistente con el hallazgo previo.
- Modelo de precios: **pago por uso**.
- La fuente **no menciona explícitamente S3/DynamoDB** como stack de persistencia en el cuerpo extraído (a diferencia del hallazgo previo, que sí lo afirmaba); esa afirmación específica de arquitectura técnica **no queda confirmada por esta fuente** y requeriría el workshop técnico (edi-workshop.awsworkshop.io) para verificarse — no se llegó a leer esa página en esta ronda.

### 2.4 PPDM 3.9

La página oficial de PPDM confirma:

- El modelo cubre **"over 60 subject areas"** usando lenguaje de definición de datos relacional — cifra más precisa que el genérico "modelo relacional estándar" del barrido previo.
- Valor estimado: **más de US$100 millones**, calculado sumando el tiempo profesional de expertos de la industria que colaboraron en su desarrollo — esto **confirma** el finding previo.
- Alcance: guías de referencia para **20+ áreas temáticas**, incluyendo pozos, datos sísmicos, reservas, reportes de producción, estratigrafía, contratos, derechos de tierra y operaciones de pozo — cubriendo el ciclo de vida completo desde exploración hasta operaciones.
- La fuente **no precisa la cifra de "24 años de desarrollo"** citada en el barrido previo; solo indica que el modelo "ha evolucionado y continúa siendo mejorado" a través de colaboración continua — esa cifra de 24 años **queda sin confirmar por esta lectura** y debería tratarse como aproximada.

### 2.5 Energistics: WITSML / PRODML / RESQML

La página de Energistics confirma el alcance de **WITSML**: cubre específicamente **"Drilling, Completions and Interventions"**, incluyendo transferencia de datos de sitio de pozo a oficina, equipos de completación, eventos, flujos, registros de pozo (wireline y LWD/logging-while-drilling) y trayectorias planificadas/calculadas. Confirma también el protocolo recomendado: **ETP (Energistics Transfer Protocol)**, descrito como "el método recomendado para asegurar transferencias continuas de datos en tiempo casi real" — esto valida el finding previo.

Dato histórico adicional no capturado antes: WITS (predecesor de WITSML, de mediados de los años 80) usaba formato binario, mientras que WITSML es "basado en web y construido con tecnología XML, independiente de plataforma y lenguaje" — útil para justificar en el proyecto por qué se opta por representar datos de perforación en JSON/Parquet en vez de reproducir XML/WITSML real.

La página **no detalla el alcance específico de PRODML ni RESQML** más allá de mencionarlos en la navegación — el barrido previo ("PRODML cubre desde el límite reservorio-pozo hasta el punto de transferencia de custodia; RESQML cubre el modelo de reservorio") **no pudo confirmarse ni refutarse** con esta página puntual; sería necesario visitar las páginas específicas de cada estándar dentro de energistics.org (no leídas en esta ronda por restricción de alcance).

## 3. Tabla comparativa: implementaciones gestionadas de OSDU

| Dimensión | AWS Energy Data Insights (EDI) | Azure Data Manager for Energy (ADME) |
|---|---|---|
| Modelo | Managed Service (AMS) con soporte 24/7 | PaaS totalmente gestionado (Microsoft) |
| Partner de dominio | No especificado en la fuente | SLB (colaboración explícita) |
| Regiones | 7 (Virginia, Oregon, Singapore, Sydney, Irlanda, París, São Paulo) | No cuantificado en la fuente leída |
| Autenticación | No detallada en el anuncio | Microsoft Entra ID |
| Particionamiento de datos | No mencionado en el anuncio | Múltiples particiones por instancia, creables post-despliegue |
| Integración de ecosistema | No detallada | SharePoint, Synapse, Power BI (conector liberado), Petrel |
| Beneficio cuantificado | Ingesta acelerada "de semanas a horas" | Upgrades continuos de milestone gestionados por Microsoft |
| Precio | Pago por uso | No especificado en la fuente |

Ninguna de las dos ofertas tiene tier gratuito relevante para un proyecto portfolio — ambas están orientadas a clientes empresariales de la industria energética, no a desarrolladores individuales. Esto refuerza que el proyecto portfolio de YPF **no debe intentar usar EDI ni ADME reales**, sino construir su propio lakehouse genérico inspirado en el modelo conceptual OSDU/PPDM, sobre AWS Free Tier o Databricks Free Edition.

## 4. Repo plantilla: 1ambda/lakehouse (README completo)

La lectura del README confirma el stack de versiones:

| Componente | Versión mínima confirmada en README |
|---|---|
| Trino | 425+ |
| dbt | 1.5+ |
| Spark | 3.3+ |
| Flink | 1.16+ |
| Iceberg | 1.3.1+ |
| Hudi | 0.13.1+ |
| Airflow | 2.7+ |
| Kafka | 3.4+ |
| Debezium | 2.3+ |
| JupyterLab | 3+ |

El proyecto usa **perfiles de Docker Compose** (`COMPOSE_PROFILES=trino|spark|flink|airflow docker-compose up`, combinables) y atajos `make compose.cdc` / `make compose.stream` para escenarios de CDC y streaming completo. **El README no documenta requisitos explícitos de RAM/CPU mínimos** — la pregunta abierta del barrido previo **sigue sin resolverse** para este repo específico; solo se puede inferir cualitativamente que, al levantar Spark + Flink + Kafka + Debezium + Trino + Airflow simultáneamente, el consumo de RAM será alto (fácilmente >16 GB si se combinan varios perfiles), dado el número de JVMs concurrentes involucradas. Para el proyecto YPF se recomienda **no levantar el stack completo** sino elegir perfiles específicos (p. ej. solo `trino` + `spark` + almacenamiento de objetos, sin Flink/Kafka si no se necesita streaming real) para mantenerse dentro de los ~8–16 GB de RAM típicos de una laptop de desarrollo. Los otros tres repos del barrido (vutrinh274/local_lakehouse, kiyeonjeon21/data-stack-lab, lechihoang/Data-lakehouse) no se releyeron en esta ronda por restricción de alcance/tiempo — la pregunta sobre sus requisitos de RAM **permanece abierta** para esos tres.

## 5. Fuente arXiv 2504.13976 (downstream retail, IA/IoT)

El intento de lectura completa vía WebFetch del PDF **falló**: el contenido llegó como datos binarios/streams de imagen corruptos, sin texto extraíble ("Cannot determine" título, autores, ni contenido técnico). El archivo binario quedó guardado localmente en la ruta de resultados de herramienta, pero no aporta información citable. **Se retracta la posibilidad de citar contenido específico de este paper** hasta poder abrirlo con un extractor de PDF dedicado (por ejemplo, descargando el PDF y usando un parser en lugar de WebFetch, o accediendo a la versión abstract/HTML en arxiv.org/abs/2504.13976 en vez de /pdf/). El finding previo ("survey sobre estación de servicio del futuro con IA/ML/IoT") se mantiene como hallazgo de nivel medium sin verificación adicional en esta ronda.

## 6. Preguntas abiertas que persisten tras esta ronda

1. **Requisitos de RAM/CPU** de los repos vutrinh274/local_lakehouse, kiyeonjeon21/data-stack-lab y lechihoang/Data-lakehouse — no releídos en esta ronda; 1ambda/lakehouse tampoco documenta cifra explícita, solo permite inferir necesidad de recursos sustanciales por la cantidad de servicios JVM concurrentes.
2. **Publicación técnica propia de YPF** sobre la arquitectura del RTIC o el modelo predictivo — no localizada; toda la cobertura disponible es periodística, no un paper/whitepaper corporativo.
3. **Madurez desplegable de la OSDU community edition** en GitLab (`community.opengroup.org/osdu`) fuera de las ofertas gestionadas — se confirmó la existencia del repositorio pero no se auditó su instalación local, issues, o esfuerzo de setup.
4. **Type curves segmentadas por yacimiento/formación específicas de Vaca Muerta** publicadas por YPF u otras operadoras — no verificado en esta ronda (no se pudo re-buscar por agotamiento del presupuesto de WebSearch).
5. **Benchmarks de precisión (RMSE/MAPE)** de los papers de DCA con LSTM/GRU/RNN frente a Arps/Duong/SEDM — no verificado; el acceso a OnePetro requeriría lectura del paper completo (frecuentemente detrás de paywall), no intentada en esta ronda.
6. **Nueva pregunta que surge de esta lectura**: la discrepancia entre "más de 2.000 pozos monitoreados" (barrido previo) y "20 equipos de torre" (lectura completa del mismo artículo) sobre el RTIC de YPF debe resolverse releyendo manualmente el artículo original en el navegador, ya que el extractor automático pudo haber perdido contexto entre ambas cifras (posiblemente una se refiere al total histórico de pozos de la compañía y la otra a la operación activa en tiempo real).

## 7. Recomendaciones de diseño derivadas para el proyecto portfolio YPF

- **No replicar OSDU/PPDM/WITSML literalmente**: son estándares para gobernanza empresarial de datos de E&P a escala de operadora real, con complejidad de despliegue (Kubernetes, microservicios Java) incompatible con Free Tier. En su lugar, tomar **prestado el vocabulario conceptual** (entidades tipo Well/Wellbore/WellLog de OSDU, áreas temáticas de PPDM como pozos/producción/reservas) para nombrar y estructurar las tablas silver/gold del lakehouse propio.
- **Usar ETP/WITSML como justificación de diseño**, no como protocolo real a implementar: documentar en el README del proyecto que el formato de ingesta simulará streams de datos de perforación "al estilo WITSML" pero en JSON/Avro sobre Kafka o Kinesis, evitando la complejidad XML real.
- **Inspirarse en el caso RTIC de YPF** (monitoreo remoto, +60 variables por pozo, modelo predictivo sobre histórico de pozos, asistente conversacional) como narrativa de producto para el proyecto, dejando explícito que no hay arquitectura técnica pública de YPF que replicar fielmente — es una aproximación de buenas prácticas de industria.
- **Elegir con cuidado los perfiles Docker Compose** del repo 1ambda/lakehouse (o equivalente) para no exceder la RAM disponible: priorizar Trino + Spark + Iceberg/MinIO sobre Flink + Kafka + Debezium si el objetivo es batch/medallion antes que streaming real.
- **Mantener como líneas de investigación pendientes** los benchmarks cuantitativos de DCA con deep learning y las type curves específicas de Vaca Muerta, dado que no se pudieron verificar en esta ronda — no incluir cifras de RMSE/MAPE en el diseño del proyecto hasta confirmarlas con lectura directa de los papers en OnePetro/SPE.

## Fuentes

1. https://www.opengroup.org/osdu-forum/
2. https://learn.microsoft.com/en-us/azure/energy-data-services/overview-microsoft-energy-data-services
3. https://aws.amazon.com/about-aws/whats-new/2025/05/managed-support-energy-data-insights/
4. https://energistics.org/witsml-data-standards
5. https://ppdm.org/ppdm/PPDM/IEDS/PPDM_Data_Model/PPDM/PPDM_3.9_Data_Model.aspx?hkey=c8aed1ca-aa85-409e-8d89-74b42a6d2a18
6. https://arxiv.org/pdf/2504.13976 (lectura fallida — PDF no legible por el extractor; contenido no citable)
7. https://github.com/1ambda/lakehouse
8. https://mase.lmneuquen.com/vaca-muerta/como-es-el-real-time-intelligence-center-el-cerebro-digital-ypf-que-construye-los-pozos-vaca-muerta-n1161568
9. https://community.opengroup.org/osdu (repositorio OSDU open source mencionado por la fuente 1, no auditado directamente en esta ronda)

# Databricks Free Edition y SaaS gratuitos para un proyecto portfolio YPF Data Engineering

**Fecha de investigación:** 2026-09-04
**Alcance:** Lectura profunda de fuentes primarias sobre límites de Databricks Free Edition y de un conjunto de SaaS gratuitos (Neon, Supabase, Grafana Cloud, GitHub Actions), con verificación de findings del barrido previo, para diseñar un proyecto portfolio de Data Engineering end-to-end sobre YPF que corra en nubes gratuitas o en local distribuido.

---

## 1. Databricks Free Edition: límites verificados en la fuente primaria

Se leyó completa la página oficial `free-edition-limitations` (fuente 1) y la página general de límites de recursos `resources/limits` (fuente 2). La lectura **confirma** casi todos los findings del barrido previo y aporta precisión textual adicional.

### 1.1 Cómputo y recursos serverless (confirmado)

| Recurso | Límite exacto (cita textual) | Fuente |
|---|---|---|
| SQL Warehouse | "One SQL warehouse, limited to a `2X-Small` cluster size" | free-edition-limitations |
| Notebooks/compute serverless | "Limited compute size and usage" (sin cifra numérica publicada) | free-edition-limitations |
| Jobs (Lakeflow Jobs) | "Max of 5 concurrent job tasks per account" | free-edition-limitations |
| Lakeflow Declarative Pipelines | "One active pipeline per pipeline type" | free-edition-limitations |
| AI Search (Vector Search) | "One AI Search endpoint, limited to one search unit" | free-edition-limitations |
| Databricks Apps | "Up to 3 Databricks Apps per account", con ejecución "up to 24 hours after being started, updated, or redeployed" | free-edition-limitations |
| Lakebase (Postgres gestionado) | "One Lakebase project per account" | free-edition-limitations |
| Workspace / metastore | "One workspace and one metastore per account" | free-edition-limitations |

Esto confirma punto por punto los findings previos marcados `[high]` sobre SQL warehouse, jobs, pipelines, AI Search, Apps y Lakebase. La cita textual "Limited compute size and usage" para notebooks serverless es la única referencia a cómputo de notebooks, y **no incluye ninguna cifra de GB o TB de storage total del workspace** — la pregunta abierta sobre storage total sigue sin respuesta en esta fuente primaria; no hay una cifra publicada.

### 1.2 Uso comercial y ciclo de vida de la cuenta (confirmado)

La fuente confirma textualmente: "Free Edition accounts may not be used for commercial purposes" — hallazgo previo validado sin matices adicionales. La página no detalla en el extracto obtenido las cláusulas exactas sobre borrado por inactividad ni el mecanismo de "fair-use throttling" descripto en el barrido previo (el extracto de esta lectura no las reprodujo textualmente, aunque no las contradice); se recomienda tratar esos dos puntos como parcialmente confirmados por el barrido previo, no re-verificados palabra por palabra en esta pasada.

### 1.3 Lo que la fuente primaria NO aclara (preguntas abiertas que persisten)

La lectura directa de `free-edition-limitations` **no contiene** información cuantitativa sobre:
- Cifra de storage total por workspace (GB/TB) — **sigue sin publicarse**.
- Si "no custom workspace storage locations" bloquea completamente la creación de external locations de Unity Catalog apuntando a un bucket S3 propio del usuario — la fuente no lo aclara de forma inequívoca en el extracto disponible.
- Detalles cuantitativos de Lakeflow Connect (conectores disponibles/no disponibles) en Free Edition.
- Límites específicos de Genie o AI/BI Dashboards por edición.

Estas cuatro preguntas abiertas del barrido previo **no pudieron cerrarse** en esta pasada: el presupuesto de búsquedas web (WebSearch) de la sesión se agotó (200/200) antes de poder lanzar las búsquedas adicionales dirigidas a Streamlit Community Cloud, external locations S3, Lakeflow Connect y Genie/AI-BI. Quedan como preguntas abiertas para una siguiente sesión de investigación, con la recomendación concreta de consultar directamente `docs.databricks.com/aws/en/connect/*` (Lakeflow Connect) y `docs.databricks.com/aws/en/genie/*`, y de probar empíricamente (crear un external location apuntando a S3 propio dentro de una cuenta Free Edition real) ya que la documentación no zanja el punto.

### 1.4 Límites de recursos a nivel workspace (contexto, no específico de Free Edition)

La página `resources/limits` (fuente 2) documenta topes generales de Databricks que aplican como techo superior en cualquier workspace (no exclusivos de Free Edition, y en la práctica Free Edition está muy por debajo de estos números por las restricciones de cómputo serverless ya vistas):

| Recurso | Límite |
|---|---|
| Jobs guardados por workspace | 12,000 |
| Tasks corriendo simultáneamente | 2,000 (excluyendo parent tasks) |
| Parent tasks simultáneas | 750 |
| Pipelines guardados | 12,000 |
| Tablas / volúmenes / funciones por schema (Unity Catalog) | 10,000 cada uno |
| Columnas por tabla | 32,768 |
| Modelos registrados por schema | 1,000 |
| MLflow experiment runs | 500,000 por experimento |
| MLflow registered models | 100,000 por workspace |
| SQL warehouses por workspace | 1,000 |
| Databricks Apps por workspace | 100 |
| Secret scopes | 1,000 |

**Nota de interpretación importante para el diseño del proyecto:** estos números (12,000 jobs, 100 apps, 1,000 SQL warehouses) son topes de *arquitectura de plataforma*, no promesas de cuota para Free Edition — la cuenta Free Edition ya está limitada aguas arriba por "1 SQL warehouse" y "3 apps" según la sección 1.1. No deben citarse como si fueran límites de Free Edition; se incluyen aquí solo como contexto de qué es "la plataforma completa" versus "lo que da Free Edition".

### 1.5 Migración de Community Edition a Free Edition (confirmado, con matiz)

La fuente `ce-migration` (fuente 3) confirma: "Free Edition replaced the legacy Databricks Community Edition, which was retired in 2025" y "If you previously used Community Edition, sign up for Free Edition to continue your work." **No hay detalle textual disponible en el extracto sobre migración automática de datos o notebooks** — la redacción sugiere que es un registro nuevo ("sign up... to continue your work"), no una migración automática de cuenta. Esto matiza el finding previo: no hay evidencia de que notebooks/datos de Community Edition se trasladen automáticamente a Free Edition; lo verificado es solo la sucesión de producto, no la portabilidad de datos. Relevante para el diseño: **no asumir continuidad de datos entre ediciones** al planificar cualquier prueba de concepto histórica.

### 1.6 Incidente de Asset Bundles / Terraform (confirmado con precisión adicional)

La lectura del hilo de comunidad (fuente 4) aporta el error textual exacto:

```
Error: error downloading Terraform: Get "https://releases.hashicorp.com/terraform/1.5.5/index.json":
dial tcp: lookup releases.hashicorp.com on 192.168.200.5:53: server misbehaving
```

Un colaborador de la comunidad (no personal oficial de Databricks) respondió que "Databricks Free edition has outbound internet access restricted to a limited set of trusted domains", identificando esto como causa probable. **No hay una declaración oficial de Databricks** confirmando o negando el bloqueo de `releases.hashicorp.com` como política — es una hipótesis de comunidad, razonable dado el error DNS, pero no una confirmación corporativa. El finding previo debe mantenerse marcado `[medium]`, no `[high]`, exactamente como estaba: es evidencia de un caso reproducido, no una política documentada oficialmente. **Implicación práctica directa para el proyecto YPF**: si el diseño contempla usar Databricks Asset Bundles (Terraform-based IaC) para desplegar el pipeline en Free Edition, hay riesgo concreto y documentado de que el despliegue falle por restricción de red saliente; conviene diseñar el despliegue vía UI/CLI de Databricks o notebooks en vez de depender de Asset Bundles como camino único, o tener un plan B (despliegue manual) documentado.

---

## 2. SaaS gratuitos: cifras verificadas en fuentes primarias

### 2.1 Neon (Postgres serverless) — confirmado con precisión

| Parámetro | Valor exacto |
|---|---|
| Storage | "0.5 GB/project" |
| Cómputo | "100 CU-hours/project" al mes |
| Proyectos totales | hasta 100 |
| Branches por proyecto | 10 |
| Autosuspend | "After 5 min", **no se puede desactivar** en el plan Free |

Esto confirma el finding previo sin cambios: 0.5 GB por proyecto (no por cuenta), 100 CU-hours/mes, 100 proyectos, 10 branches, autosuspend fijo a 5 min.

### 2.2 Supabase — confirmado con precisión

| Parámetro | Valor exacto |
|---|---|
| Tamaño de base de datos | "500 MB database size" (Shared CPU, 500 MB RAM) |
| File storage | "1 GB" |
| Egress | "5 GB egress" + "5 GB cached egress" (son dos cupos separados) |
| Edge Functions | "500,000 included" invocaciones |
| Auth MAU | "50,000 monthly active users" |
| Proyectos activos | "Limit of 2 active projects" |
| Pausa por inactividad | "after 1 week of inactivity" |

Confirma el finding previo. Dato adicional no capturado antes: el egress cacheado (5 GB) es una cuota **separada** del egress normal (5 GB), es decir 10 GB combinados de transferencia saliente, dato útil para dimensionar cuánto tráfico puede soportar un dashboard o API expuesta desde Supabase en el proyecto YPF.

### 2.3 Grafana Cloud — confirmado, con matices sobre traces/profiles

| Parámetro | Valor exacto |
|---|---|
| Métricas | "10k active series per month", retención 14 días |
| Logs | "50 GB ingested per month", retención 14 días |
| Traces | "50 GB ingested per month", retención 14 días |
| Profiles | "50 GB ingested per month", retención 14 días |
| Usuarios de Grafana Assistant (IA) | "3 active AI users per month", 40M tokens/usuario |
| Soporte | "Community support" |

El barrido previo decía "50GB/mes de logs, traces y profiles **cada uno**" — la lectura confirma esto exactamente (tres cupos separados de 50 GB, no uno compartido). Dato nuevo relevante: Grafana Cloud Free incluye un asistente de IA con 3 usuarios activos/mes y 40M tokens por usuario, útil si el proyecto YPF quiere observabilidad con asistencia de IA sin costo adicional.

### 2.4 GitHub Actions — confirmado y ampliado

| Plan | Minutos/mes | Artifact storage |
|---|---|---|
| Free (personal/org) | 2,000 | 500 MB |
| Pro | 3,000 | 1 GB |
| Team | 3,000 | 2 GB |
| Enterprise Cloud | 50,000 | 50 GB |

Cache: "10 GB por repositorio" en todos los planes (asignación separada, no acumulable entre repos).
Repos públicos: "GitHub Actions usage is free for self-hosted runners and for public repositories that use standard GitHub-hosted runners" — confirma que un repo público del proyecto YPF tendría minutos de CI/CD **ilimitados** con runners estándar, lo cual es una recomendación de diseño directa: si el proyecto se publica como repo público en GitHub (razonable para un portfolio), el pipeline de CI/CD no consume la cuota de 2,000 minutos.

---

## 3. Correcciones y descartes de findings previos

No se encontró ningún finding previo que la lectura profunda contradiga o descarte. Todos los findings de las 8 fuentes leídas fueron **confirmados textualmente**, con dos matices importantes documentados arriba:

1. El bloqueo de Terraform/HashiCorp en Free Edition es una **hipótesis de comunidad respaldada por un error reproducido**, no una política oficial confirmada por Databricks — mantener como `[medium]`, no elevar a `[high]`.
2. La migración de Community Edition a Free Edition parece ser un **registro nuevo**, no una migración automática de datos — no asumir portabilidad de notebooks/datos entre ediciones.

---

## 4. Preguntas abiertas que persisten (no resueltas en esta pasada)

El presupuesto de búsquedas web de la sesión (200/200) se agotó antes de poder investigar las 5 búsquedas adicionales planificadas. Quedan sin resolver:

1. **Cifra exacta de storage total por workspace en Databricks Free Edition** — no publicada en la documentación oficial revisada.
2. **Si "no custom workspace storage locations" bloquea crear external locations de Unity Catalog hacia un bucket S3 propio** — ambigüedad no resuelta por la fuente primaria; requiere prueba empírica directa en una cuenta Free Edition o consulta a soporte/comunidad.
3. **Límites cuantitativos de Streamlit Community Cloud** (apps privadas, CPU/RAM) — no verificado en esta pasada.
4. **Detalles cuantitativos de Lakeflow Connect en Free Edition** (qué conectores están disponibles/bloqueados) — no verificado.
5. **Límites de Genie y AI/BI Dashboards específicos de Free Edition** — no verificado; la documentación general de Genie no distingue por edición.

**Recomendación operativa**: antes de comprometer el diseño del proyecto YPF a un patrón que dependa de estos puntos (por ejemplo, usar S3 como almacenamiento de datos crudos con Unity Catalog external location, o depender de un conector Lakeflow Connect específico para ingesta), validar empíricamente creando una cuenta Free Edition de prueba y probando el flujo concreto, dado que la documentación oficial no cierra estos puntos.

---

## 5. Implicaciones de diseño para el proyecto portfolio YPF

Basado en lo confirmado:

- **Un solo SQL warehouse 2X-Small y 5 tareas de job concurrentes** hacen inviable cualquier arquitectura con múltiples pipelines paralelos dentro de Free Edition; el diseño debe ser un **pipeline secuencial único** (ingesta → bronze → silver → gold) orquestado con **una sola Lakeflow Pipeline activa**, no múltiples pipelines por dominio de datos (ej. separar "producción de pozos" y "precios de commodities" en pipelines distintos no es viable — deben unificarse en un solo pipeline con múltiples flujos internos, o alternarse).
- **3 Databricks Apps con vida de 24h por redeploy** significa que cualquier dashboard construido como Databricks App necesita un mecanismo de "mantenimiento vivo" (redeploy programado) o aceptar que se apague y deba reiniciarse manualmente cada día — no es apto para una demo "siempre online" sin automatización adicional.
- **Riesgo documentado de fallo de Terraform/Asset Bundles** recomienda evitar IaC basado en Databricks Asset Bundles como único método de despliegue; usar Databricks CLI directo o el UI, con Asset Bundles como opcional/best-effort.
- **No hay portabilidad garantizada de datos entre ediciones** de Databricks — si el proyecto se pausa o la cuenta se recrea, no asumir que notebooks/tablas persisten; documentar el proyecto de forma que sea reproducible desde cero (scripts de setup, no dependencia de estado manual).
- **GitHub Actions es gratis e ilimitado en runners estándar si el repo es público** — para un proyecto portfolio (que típicamente se publica público en GitHub para mostrarlo a reclutadores), esto elimina la preocupación por los 2,000 minutos/mes del plan privado; conviene publicar el repo del proyecto YPF como público.
- **Neon y Supabase como alternativas a Lakebase** para la capa transaccional/serving: Neon da 0.5 GB por proyecto con hasta 100 proyectos (permite separar entornos dev/staging/prod como proyectos distintos sin costo), mientras Supabase da 500 MB de DB más autenticación (50k MAU) y Edge Functions (500k invocaciones) integradas, útil si el proyecto necesita un backend API además de la base de datos — pero ambos tienen autosuspend agresivo (5 min en Neon, pausa a la semana en Supabase) que debe considerarse si se necesita disponibilidad constante para una demo en vivo.
- **Grafana Cloud Free** es suficiente para observabilidad del pipeline (métricas de jobs, logs de ejecución) dado que 10k series y 50 GB/mes de logs exceden holgadamente lo que generaría un proyecto portfolio de este tamaño.

---

## 6. Fuentes

Leídas completas con WebFetch en esta sesión:

1. https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
2. https://docs.databricks.com/aws/en/resources/limits
3. https://docs.databricks.com/aws/en/getting-started/ce-migration
4. https://community.databricks.com/t5/administration-architecture/asset-bundle-on-free-edition/td-p/127236
5. https://neon.com/docs/introduction/plans
6. https://supabase.com/pricing
7. https://grafana.com/pricing/
8. https://docs.github.com/en/billing/concepts/product-billing/github-actions

No se pudieron realizar búsquedas adicionales para cerrar preguntas abiertas: el presupuesto de WebSearch de la sesión (200/200) se agotó. Las fuentes citadas en la sección de "Contexto del barrido previo" del prompt original (Snowflake, MotherDuck, Confluent Cloud, Redpanda, Aiven, Astronomer, Prefect, Dagster+, dbt Cloud, Kestra, Hugging Face Spaces, Render, Fly.io, community.databricks.com sobre GPU quota y cuentas bloqueadas) no fueron re-leídas en esta pasada — se mantienen tal como estaban en el barrido previo, sin nueva verificación.

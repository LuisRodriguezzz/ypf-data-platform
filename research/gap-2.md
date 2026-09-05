# GAP-2: Databricks Free Edition y egress a `datos.energia.gob.ar`

> Investigación con fuentes primarias (docs.databricks.com). Nota metodológica: el presupuesto de `WebSearch` de esta sesión se agotó tras la primera búsqueda, por lo que no se pudieron rastrear hilos de `community.databricks.com` / Stack Overflow con fecha para los puntos 2, 5 y 7. Esos puntos quedan explícitamente como **NO DOCUMENTADO** (no encontrado), no como negativos confirmados.

---

## 1. Texto de `free-edition-limitations` — ¿allowlist publicada?

**Veredicto: CONFIRMADO (la restricción existe) / NO DOCUMENTADO (la lista de dominios no está publicada)**

Cita textual recuperada de la página oficial:

> "Custom compute configurations are not supported. Additionally, outbound internet access is restricted to a limited set of trusted domains."

La página **no enumera los dominios** de esa allowlist en ningún punto del texto ni en las páginas hermanas revisadas (`free-edition`, `free-trial-vs-free-edition`). Tampoco existe una tabla o anexo con la lista — se describe de forma puramente genérica ("a limited set of trusted domains").

Otras citas relevantes de la misma página:

> "Custom workspace storage locations" — no soportadas (sin cifra de storage asociada).

> "Free Edition accounts may not be used for commercial purposes." / "are meant for non-commercial use."

> "Databricks may delete Free Edition accounts that are inactive for a prolonged period." (sin plazo numérico)

> "Each account is subject to the Databricks fair usage policy" — si se exceden cuotas, "your workspace's compute resources will be shut down and unavailable for the rest of the day (and in extreme cases, the rest of the month)."

> Lakeflow Declarative Pipelines: "One active pipeline per pipeline type."

URL: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

Páginas hermanas revisadas (enlazadas desde la propia página, sin contenido adicional sobre la allowlist):
- https://docs.databricks.com/aws/en/getting-started/free-edition
- https://docs.databricks.com/aws/en/getting-started/free-trial-vs-free-edition
- Versión Azure equivalente (enlazada, no fetchada en esta corrida): https://learn.microsoft.com/azure/databricks/getting-started/free-edition-limitations

---

## 2. ¿LinkedIn habilita acceso arbitrario o solo amplía la allowlist?

**Veredicto: NO DOCUMENTADO (documentación oficial) / REPORTADO POR USUARIOS: no verificado esta corrida**

La página oficial solo dice que la verificación por LinkedIn "amplía" el acceso saliente, sin especificar si el resultado es una allowlist más grande o acceso libre. No hay cita textual que use la palabra "arbitrary"/"any domain" — el lenguaje se mantiene en "trusted domains" incluso tras la verificación, según el resumen de la página madre.

No fue posible confirmar con evidencia empírica (hilos de comunidad, Stack Overflow, capturas de `requests.get()`/`pd.read_csv('http://...')`) si tras la verificación funciona HTTP plano (puerto 80, sin TLS) hacia un dominio arbitrario como `datos.energia.gob.ar`, porque el presupuesto de `WebSearch` de la sesión se agotó antes de poder rastrear esos hilos. Un intento de usar Bing vía `WebFetch` como sustituto no devolvió URLs de hilos concretos, solo un resumen genérico de la página de resultados.

**Esto queda como pregunta abierta sin resolver — recomiendo repetir la búsqueda en una sesión con presupuesto de WebSearch disponible**, buscando específicamente: `community.databricks.com Free Edition network egress requests` y `Free Edition LinkedIn verify internet access site:community.databricks.com`.

---

## 3. Límite de storage (GB/TB)

**Veredicto: NO DOCUMENTADO**

Ninguna de las páginas oficiales revisadas (`free-edition-limitations`, `free-edition`, `free-trial-vs-free-edition`) publica una cifra de storage en GB o TB para el workspace de Free Edition. La comparación con Free Trial menciona "daily usage limits" y "limited compute size" de forma genérica, sin números. No se pudo buscar reportes empíricos de usuarios (Stack Overflow/community) por el mismo límite de presupuesto de búsqueda del punto 2.

URL: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

---

## 4. External locations / storage credentials hacia S3 propio

**Veredicto: CONFIRMADO que está bloqueado (a nivel de "workspace storage"), zona gris sobre Unity Catalog external locations específicamente**

Cita textual: "Custom workspace storage locations" figuran explícitamente como no soportadas en `free-edition-limitations`.

La documentación general de Unity Catalog sobre external locations (https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations) no menciona restricciones específicas de Free Edition — es la página genérica del producto, sin distinción por edición. No se encontró una página que confirme o descarte explícitamente si se puede crear un `storage credential`/`external location` apuntando a un bucket S3 propio dentro de Free Edition; dado que "custom workspace storage locations" está prohibido de forma general, la lectura más conservadora es que external locations personalizadas también están bloqueadas, pero **no hay una cita que lo diga en esos términos exactos** — se declara zona gris / no documentado explícitamente.

No se hallaron reportes empíricos de usuarios sobre este punto (mismo límite de búsqueda).

---

## 5. Disponibilidad de herramientas/servicios en Free Edition

**Veredicto: mayormente NO DOCUMENTADO** (ninguna de las páginas de producto revisadas distingue por edición)

| Herramienta | Estado | Evidencia |
|---|---|---|
| Databricks Asset Bundles / CLI / Terraform provider | NO DOCUMENTADO | La doc del CLI (https://docs.databricks.com/aws/en/dev-tools/cli/) no menciona Free Edition en absoluto. El reporte de fallo de descarga de `releases.hashicorp.com` mencionado en la consigna no pudo verificarse por falta de presupuesto de búsqueda. |
| MLflow tracking + model registry | NO DOCUMENTADO | No se encontró mención en las páginas fetchadas. |
| Model Serving endpoints | NO DOCUMENTADO (mencionado como feature del producto general, enlazado desde la página `free-edition`, pero sin aclarar límites) | Enlace visto: `/aws/en/machine-learning/model-serving/` |
| Lakeflow Declarative Pipelines | **CONFIRMADO disponible con límite**: "One active pipeline per pipeline type." | https://docs.databricks.com/aws/en/getting-started/free-edition-limitations |
| Auto Loader / `cloudFiles` | NO DOCUMENTADO | Sin mención explícita en las páginas revisadas. |
| Structured Streaming (`availableNow`/`processingTime`) | NO DOCUMENTADO | Sin mención explícita. |

---

## 6. Cláusula de uso comercial y proyecto de portfolio

**Veredicto: NO ACLARADO — zona gris confirmada**

Cita textual: "Free Edition accounts may not be used for commercial purposes" y "are meant for non-commercial use" (fuente: `free-edition-limitations` / `free-edition`).

No se encontró ninguna aclaración oficial sobre si un proyecto de portfolio personal usado para conseguir empleo cuenta como "uso comercial" o no. Es zona gris: un portfolio no vende nada ni genera ingresos directos, pero su propósito instrumental (conseguir empleo) podría interpretarse en cualquier dirección según quien lo lea. Databricks no define el término "commercial purposes" en ninguna de las páginas revisadas.

---

## 7. Borrado por inactividad y fair-use throttling

**Veredicto: CONFIRMADO que las políticas existen / NO DOCUMENTADO en cifras concretas**

Cita textual: "Databricks may delete Free Edition accounts that are inactive for a prolonged period." — sin plazo numérico (¿30 días? ¿90? no se especifica).

Cita textual sobre fair-use: "Each account is subject to the Databricks fair usage policy" y, si se exceden cuotas, "your workspace's compute resources will be shut down and unavailable for the rest of the day (and in extreme cases, the rest of the month)." — tampoco hay cifras concretas de qué cuota dispara esto (ej. horas de cómputo, GB procesados).

URL: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

---

## CONSECUENCIA DE DISEÑO

**¿Puede Databricks Free Edition descargar por sí mismo archivos desde `http://datos.energia.gob.ar`?**

Con la evidencia disponible: **incierto, pero de alto riesgo de que NO funcione**, por dos razones independientes, cualquiera de las cuales basta para bloquear la ingesta directa:

1. **Egress restringido a una allowlist no publicada de dominios "trusted".** `datos.energia.gob.ar` casi con certeza no está en esa lista (es un portal CKAN gubernamental argentino, no un servicio con el que Databricks tendría partnership). No hay forma de verificarlo ni de solicitarlo — la lista no es pública ni autoservicio.
2. **El origen fuerza HTTP plano** (redirect 301 de HTTPS a HTTP, `Strict-Transport-Security: max-age=0`). Incluso si el dominio estuviera en la allowlist, no hay evidencia de que el egress permitido incluya tráfico HTTP sin TLS en puerto 80 — es razonable asumir que un allowlist de seguridad empresarial exigiría HTTPS, lo que rompería la conexión a un servidor que solo sirve HTTP plano.

No se pudo confirmar empíricamente ninguno de los dos puntos por falta de presupuesto de búsqueda en esta sesión — **recomiendo verificarlo de forma directa y barata**: crear una cuenta Free Edition, verificar LinkedIn, y ejecutar `requests.get('http://datos.energia.gob.ar/...')` en un notebook. Esa prueba de 10 minutos vale más que cualquier búsqueda documental adicional, dado que Databricks no publica la allowlist.

**Mientras tanto, diseñar asumiendo que la ingesta directa desde Databricks Free Edition NO es viable.** Dos arquitecturas alternativas concretas:

### Alternativa A — GitHub Actions como capa de ingesta, Databricks como capa de transformación/serving
- Un workflow de GitHub Actions (runner con egress libre) hace `curl`/`requests` contra `http://datos.energia.gob.ar`, descarga los CSV/recursos CKAN, y los sube a:
  - un Volume de Unity Catalog vía Databricks REST API / Databricks CLI (`databricks fs cp` o el API de Files), o
  - un bucket S3 intermedio si se resuelve que external locations personalizadas sí funcionan (punto 4, aún zona gris) — si no, usar directamente el Volume administrado de Free Edition, que sí es soportado (solo "custom workspace storage locations" está bloqueado, no los Volumes por defecto del workspace).
- Databricks Free Edition entra recién después: Lakeflow Declarative Pipelines / notebooks leen desde el Volume y hacen transformación → Delta. Cron en GitHub Actions (ej. diario) reemplaza al Auto Loader que no se pudo confirmar disponible.
- Ventaja: no depende de resolver la allowlist de egress; GitHub Actions es gratis para repos públicos y confiable para HTTP plano.

### Alternativa B — Ingesta local (Docker) + push a Delta/Volume
- Un contenedor Docker corriendo localmente (o en cualquier VM propia con egress sin restricciones) descarga los datasets de CKAN con `requests`/`ckanapi`, valida/normaliza, y sube los archivos resultantes al workspace de Databricks vía Databricks CLI (`databricks fs cp`, o `databricks-connect`/SDK) o directamente escribiendo a un Volume expuesto por API.
- Esto además resuelve el problema de "storage no publicado": el volumen de datos crudo se controla y filtra localmente antes de subir, evitando sorpresas de cuota en Free Edition.
- Ventaja sobre A: control total del entorno de ingesta (reintentos, logging, parseo de metadata CKAN complejo) sin las limitaciones de un runner efímero de CI; más parecido a un patrón real de "edge/on-prem ingestion + cloud lakehouse" que se puede documentar como decisión de arquitectura en el portfolio.

En ambos casos, Databricks Free Edition queda relegado a las capas de **transformación, gobernanza (Unity Catalog), consulta (SQL Warehouse serverless) y eventualmente serving/ML**, nunca como responsable de la ingesta cruda desde una fuente HTTP externa no confiable/no confirmada en la allowlist.

---

### Fuentes citadas
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- https://docs.databricks.com/aws/en/getting-started/free-edition
- https://docs.databricks.com/aws/en/getting-started/free-trial-vs-free-edition
- https://docs.databricks.com/aws/en/compute/serverless/limitations
- https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations
- https://docs.databricks.com/aws/en/dev-tools/cli/
- https://docs.databricks.com/aws/en/release-notes/ (sin entradas específicas de Free Edition encontradas en el índice)

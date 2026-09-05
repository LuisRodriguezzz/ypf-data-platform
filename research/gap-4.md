# Gap 4 — Evidencia primaria del stack de datos/TI de YPF (fuentes regulatorias y corporativas)

**Fecha de investigación:** 2026-09-04
**Nota metodológica:** el presupuesto de `WebSearch` de esta sesión se agotó antes de empezar (200/200 usados por trabajo previo), así que toda la investigación se hizo con `WebFetch` contra URLs de EDGAR (SEC), la EDGAR Full Text Search API (`efts.sec.gov`), el sitio de YPF y páginas de "customer stories" de vendors, más descarga directa (`curl`) y `grep` del Form 20-F más reciente para superar el límite de tamaño de `WebFetch` (10 MB). No se buscó en portales de empleo ni licitaciones (puntos 4 y 5 del pedido) porque dependían de `WebSearch`; queda pendiente si se libera el budget.

---

## 1. Form 20-F ante la SEC — la fuente más rica encontrada

- **CIK confirmado de YPF S.A.:** `0000904851` (coincide con lo estimado).
- 20-F más reciente (año fiscal 2025, presentado 2026-03-26): índice en
  https://www.sec.gov/Archives/edgar/data/904851/000119312526126363/0001193125-26-126363-index.htm
  documento principal: https://www.sec.gov/Archives/edgar/data/904851/000119312526126363/d95578d20f.htm
- Anterior (FY2024, presentado 2025-03-28): https://www.sec.gov/Archives/edgar/data/904851/000119312525067155/0001193125-25-067155-index.htm

Descargué el HTML completo (10.9 MB) y lo procesé con `grep`/Python para buscar cada término pedido.

### Hallazgos positivos

1. **SAP S/4HANA — implementación confirmada en 2025 (dato duro, alta confianza)**
   > "During 2025, we completed the implementation of the system SAP S/4 Hana Solutions, to replace the commercial and stock systems related to the Downstream business segment."
   — Form 20-F FY2025, sección "Changes in internal control over financial reporting".
   URL: https://www.sec.gov/Archives/edgar/data/904851/000119312526126363/d95578d20f.htm
   Fecha del documento: 2026-03-26 (año fiscal 2025).
   **Confianza: alta** — es una declaración explícita y textual de la propia compañía ante la SEC. El término "SAP" aparece en los 9 Form 20-F disponibles desde 2019 (accession numbers listados por EDGAR Full Text Search: 0001193125-19-096821 a 0001193125-26-126363), lo que sugiere que SAP es el ERP histórico de YPF, con una modernización a S/4HANA reciente en el segmento Downstream (comercial y stock).

2. **CISO y mención de "SAP Basis maintenance" como parte de su trayectoria (confirma administración interna de SAP)**
   > "...working in the areas of control and telemetry systems for the industrial world, building automation, communications, application/database/SAP Basis maintenance, electronic security systems (CCTV and access control), IT, OT, Cybersecurity and data architecture..."
   — Form 20-F FY2025, biografía del CISO (Leonardo Oscar Iglesias, en el cargo desde enero 2023, posición creada en 2021).
   **Confianza: alta** (cita textual), aunque es información biográfica, no arquitectónica.

3. **YPF Digital S.A.U.** — subsidiaria de "digital development services and solutions", dueña de la app YPF (billetera virtual). Mencionada varias veces en la sección de directorio y de descripción del negocio. No se detalla stack tecnológico de esta subsidiaria en el 20-F.
   **Confianza: alta** (existencia de la entidad), **nula** en cuanto a stack.

4. **IA mencionada de forma genérica, sin proveedor**
   > "We are adopting AI technology available via open source or commercial license agreements, and as such, third-parties can use this technology for use in their own products and services."
   > "...if we do not effectively leverage progress in digital technologies, including artificial intelligence (AI), we could be adversely affected."
   — Item 3 (Risk Factors) e Item 16K (Cybersecurity), Form 20-F FY2025.
   **Confianza: alta** en la cita, **nula** en nombrar tecnología específica (no dice qué proveedor de IA).

5. **Item 16K (Cybersecurity)** describe gobernanza (comité de Riesgo y Sustentabilidad, CISO, un "SOC OT" — centro de monitoreo de seguridad para tecnología operacional industrial), un "Corporate Risk Management Model" con "software desplegado en toda la Compañía" (sin nombrar el software), y el uso de "external providers" para pentesting (sin nombrarlos). **No menciona ningún proveedor de nube, ERP adicional, ni plataforma de datos por nombre.**

### Búsquedas sin resultado (negativas, verificadas con EDGAR Full Text Search API + grep sobre el texto completo)

| Término buscado | Resultado |
|---|---|
| "Microsoft Azure" | 0 resultados en los 20-F de YPF (EDGAR Full Text Search, todos los años) |
| "Microsoft" / "Azure" / "Amazon" / "AWS" | 0 apariciones en el texto completo del 20-F FY2025 |
| "cloud" | 0 apariciones literales en el 20-F FY2025 (aunque los 20-F FY2019–FY2023 sí combinan "cybersecurity" + "cloud" según EDGAR FTS — no se confirmó el contexto exacto de esas menciones anteriores por limitación de tiempo) |
| "ERP" (como sigla aislada) | 0 apariciones — el 20-F usa "SAP" directamente, nunca la sigla ERP |
| "Palantir", "Databricks", "Snowflake", "Kafka", "Spark", "Globant", "Corva", "data platform", "data lake", "machine learning" | 0 apariciones en el 20-F FY2025 |

**Conclusión parcial sobre el 20-F:** es la única fuente pública donde YPF nombra explícitamente una tecnología de datos/TI de forma auditable: **SAP (ERP), específicamente SAP S/4HANA implementado en 2025 para Downstream**. Todo lo demás (nube, IA, analítica) se describe en términos genéricos de riesgo regulatorio, sin nombrar proveedores.

---

## 2. Memoria Anual / Reporte de Sustentabilidad de YPF (ypf.com)

No se pudo acceder al contenido: las URLs de investors/sustentabilidad probadas devolvieron una página de error del sitio ("Lo sentimos, esta página no está disponible en este momento"), consistente con que el sitio de YPF usa rutas dinámicas/JS que `WebFetch` no puede resolver sin `WebSearch` para descubrir la URL exacta del PDF o la sección vigente. La portada (https://www.ypf.com) sí cargó y confirma que existen secciones de "Inversores", "Sustentabilidad", "Innovación", "Tecnología" e "YPF Digital" en el menú, pero no pude extraer su contenido en esta sesión.
**Confianza: sin datos** — este punto queda pendiente, no hay hallazgo ni negativo confirmado.

---

## 3. Case studies de vendors (verificado, negativo en los 3 casos consultados)

- **Microsoft Customer Stories** (https://www.microsoft.com/en-us/customers/search?sq=YPF): **"0 results"** — cita textual del buscador: *"Sorry, no results were found for your search."*
  **Confianza: alta** (negativo confirmado directamente en el buscador oficial de Microsoft).
- **Databricks Customers** (https://www.databricks.com/customers): no se identificó a YPF en el contenido de texto extraído de la página (aunque el grid de logos es una imagen/componente que el fetch no renderiza completo, por lo que esto es indicativo, no concluyente).
  **Confianza: media**.
- **AWS Case Studies** (https://aws.amazon.com/solutions/case-studies/...): YPF no aparece entre los casos listados en la página consultada (Sony, Blue Origin, Pinterest, Mercedes-Benz, Condé Nast, etc.).
  **Confianza: media** (la página es paginada/filtrable y no se recorrieron todas las páginas).
- **Globant.com/case-studies**: la página devolvió **HTTP 403 Forbidden** — no se pudo verificar.
- No se llegó a consultar Snowflake, Palantir, Google Cloud, SAP.com, AVEVA, SLB (Delfi) ni Corva.ai por agotamiento de tiempo/alcance — quedan pendientes.

---

## 4. Ofertas de empleo (Bumeran, LinkedIn, Computrabajo, etc.)

**No se pudo investigar.** Este punto depende de `WebSearch` (para descubrir avisos vigentes o cacheados), que estaba agotado. No hay hallazgo positivo ni negativo confiable — es un vacío de esta investigación, no una conclusión.

## 5. Licitaciones / pliegos de TI

**No se pudo investigar** por la misma razón (dependía de `WebSearch` para ubicar el portal de contrataciones de YPF).

---

## Síntesis de hallazgos (tabla)

| # | Tecnología/proveedor | Cita textual | URL | Fecha doc. | Confianza |
|---|---|---|---|---|---|
| 1 | SAP S/4HANA (ERP, Downstream) | "we completed the implementation of the system SAP S/4 Hana Solutions, to replace the commercial and stock systems related to the Downstream business segment" | sec.gov/Archives/edgar/data/904851/000119312526126363/d95578d20f.htm | 2026-03-26 (FY2025) | **Alta** |
| 2 | SAP (histórico, Basis/administración) | "...application/database/SAP Basis maintenance..." (bio del CISO) | ídem | 2026-03-26 | Alta (cita), baja (relevancia arquitectónica) |
| 3 | YPF Digital S.A.U. (unidad de software propia) | "digital development services and solutions through our subsidiary YPF Digital S.A.U." | ídem | 2026-03-26 | Alta (existencia), nula (stack) |
| 4 | IA genérica, sin proveedor | "adopting AI technology available via open source or commercial license agreements" | ídem | 2026-03-26 | Alta (cita), nula (proveedor) |
| 5 | Azure/Microsoft en 20-F | 0 menciones | EDGAR Full Text Search, `efts.sec.gov` | 2019–2026 | Alta (negativo) |
| 6 | AWS/Amazon en 20-F | 0 menciones | ídem | 2019–2026 | Alta (negativo) |
| 7 | Palantir/Databricks/Snowflake/Kafka/Spark/Globant/Corva en 20-F | 0 menciones | ídem | FY2025 | Alta (negativo) |
| 8 | YPF en Microsoft Customer Stories | "0 results" del buscador oficial | microsoft.com/en-us/customers/search?sq=YPF | consultado 2026-09-04 | Alta (negativo) |
| 9 | YPF en Databricks/AWS case studies | no aparece en el contenido extraído | databricks.com/customers; aws.amazon.com/solutions/case-studies | consultado 2026-09-04 | Media (negativo, no exhaustivo) |

---

## Conclusión explícita

**No hay evidencia pública verificable de que YPF use Databricks, Snowflake, Kafka, Spark, dbt, Palantir, Google Cloud, o de que su ERP corporativo sea algo distinto de SAP.** Verificado en: el Form 20-F FY2025 completo (texto íntegro descargado y buscado por palabra clave), la EDGAR Full Text Search de los 9 Form 20-F de YPF desde 2019, y los buscadores de "customer stories" de Microsoft, Databricks y AWS.

La **única tecnología de datos/TI que YPF nombra explícitamente y de forma auditable, en un documento firmado ante un regulador**, es **SAP — específicamente la migración a SAP S/4HANA completada en 2025 para el segmento Downstream (comercial y stock)**. Todo lo demás que ya tenías (Azure/Power BI para GAIA/Y-Click!, Corva para el RTIC, Globant como integrador de IA) sigue siendo la evidencia más concreta sobre la capa de aplicaciones y BI; el 20-F simplemente confirma, del lado ERP/ecosistema ampliado, que **SAP es un componente real y actual del stack de YPF**, mientras que la nube pública (Azure/AWS) no aparece nombrada como proveedor corporativo en ningún documento regulatorio — coherente con que Azure aparezca solo en el contexto específico de apps (GAIA/Y-Click!) y no como decisión de infraestructura de toda la empresa.

Quedan sin investigar (por falta de `WebSearch`): la Memoria Anual/Reporte de Sustentabilidad de ypf.com, ofertas de empleo con stack explícito, y licitaciones de TI. Estos tres frentes podrían todavía revelar nombres de plataformas de datos (Spark, Airflow, Azure Data Factory, etc.) que el 20-F —enfocado en riesgo regulatorio, no en arquitectura— no tiene por qué mencionar.

---

## Recomendación para el portfolio

Con esta evidencia, la estrategia más defendible frente a un entrevistador de YPF es:

1. **Elegí un stack "cloud-agnóstico pero con anclaje real"**: Azure (Data Factory / Databricks-on-Azure o Synapse) + Power BI, porque:
   - Azure y Power BI son las **únicas** piezas de infraestructura que aparecen ligadas a YPF en fuentes públicas (apps GAIA/Y-Click!, BI corporativo).
   - SAP S/4HANA (confirmado en el 20-F) es la fuente transaccional más probable que un pipeline de datos de YPF tendría que ingerir — un extractor SAP→lake es un caso de uso creíble y vistoso para el portfolio.
   - Sumar Corva (perforación en tiempo real, RTIC) como fuente de datos de series temporales/IoT le da al proyecto un ángulo específico de Oil & Gas que un genérico "ingeniero de datos" no tendría.

2. **Frasealo así ante el entrevistador** (evitando la afirmación falsa "esto es lo que usa YPF"):
   > "No encontré evidencia pública de que YPF use un stack de datos específico más allá de SAP como ERP —confirmado en su Form 20-F 2025, con la migración a S/4HANA— y de Azure/Power BI para algunas apps y BI. Diseñé este portfolio simulando un pipeline realista para una petrolera integrada: ingesta desde un ERP tipo SAP, datos operacionales de perforación al estilo Corva/RTIC, y consumo en Power BI, sobre Azure. No pretendo replicar la arquitectura interna de YPF —que no es pública— sino demostrar que entiendo los tipos de fuentes y la escala de datos con los que YPF probablemente trabaja."

3. Esto te protege de dos riesgos: (a) que un entrevistador te corrija ("no, nosotros no usamos X") y quedes expuesto por haber afirmado algo falso, y (b) que parezca que copiaste un stack de moda (Databricks/Snowflake) sin relación con la empresa real. Mostrar la investigación misma (este documento) como parte del portfolio es, de hecho, una señal fuerte de rigor de ingeniería de datos.

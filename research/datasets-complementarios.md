# Lectura profunda: fuentes de datos complementarias para el proyecto YPF Data Platform

Fecha de esta lectura: 2026-09-04. Este informe profundiza sobre el barrido previo de 6 bloques de fuentes complementarias (precios internacionales, macro/FX, energía, meteorología, info corporativa, datasets ML de pozos/sensores), leyendo con WebFetch las 8 páginas primarias señaladas y corrigiendo o descartando los findings que la lectura no respalda. **Nota metodológica importante**: el presupuesto de WebSearch de esta sesión se agotó (200/200 llamadas ya consumidas por trabajo previo) antes de poder ejecutar las 5 búsquedas adicionales planificadas para cerrar las preguntas abiertas. Por lo tanto, las preguntas abiertas se responden únicamente con lo obtenido vía WebFetch directo a las 8 URLs objetivo, y varias quedan sin resolución nueva (se señala explícitamente en cada caso).

## 1. Precios internacionales (EIA)

Se leyó completa `https://www.eia.gov/opendata/` (página de registro y overview de la API v2, no el browser de series). Hallazgos confirmados:

- El registro de API key es gratuito, mediante un enlace "Register" (con opción "Forgot API Key" para recuperación), y el uso está sujeto a un "API Terms of Service Agreement".
- La página declara explícitamente: *"EIA data is provided free of charge"*, sujeto a la política de copyright y reuso de EIA.
- Existen recursos adicionales no mencionados en el barrido previo: un **webinar de la API v2**, un **Excel Add-In** para consumir datos directo en hojas de cálculo, **bulk file downloads actualizados dos veces al día**, y un traductor de Series ID v1 → v2 (relevante porque muchas rutas legacy citadas en foros usan el esquema v1).
- **No se encontraron en esta página límites de rate limiting explícitos** (requests/hora o /día). Tampoco se detallan aquí las rutas exactas `petroleum/pri/spt` para Brent/WTI citadas en el barrido previo — esa información vive en el "API Browser" (`www.eia.gov/opendata/browser/`), no en la página de registro, por lo que los datos de fechas de inicio de series (Brent 20-may-1987, WTI Cushing 2-ene-1986) del barrido previo **no quedan ni confirmados ni contradichos** por esta lectura; siguen sustentados solo en el hallazgo anterior del browser.

**Implicancia de diseño**: al no haber límite de rate documentado públicamente, el pipeline de ingesta diaria debe implementarse de forma defensiva (backoff exponencial, caching local, un único job diario en vez de polling), ya que EIA puede aplicar throttling no anunciado a nivel de infraestructura (WAF/CDN) sin que conste en la documentación de producto.

## 2. Macro / FX (BCRA)

Se leyó completa `https://estadisticas-cambiarias.bcra.apidocs.ar/`. Esto **confirma con precisión los tres endpoints** ya relevados:

| Endpoint | Descripción |
|---|---|
| `GET /Maestros/Divisas` | Lista de divisas |
| `GET /Cotizaciones` | Lista de cotizaciones (todas las monedas) |
| `GET /Cotizaciones/{codMoneda}` | Cotizaciones filtradas por moneda (parámetro de ruta) |

- Base URL confirmada: `https://api.bcra.gob.ar`.
- Contacto técnico publicado: `api@bcra.gob.ar` (útil para escalar dudas de cuota).
- **La documentación OpenAPI leída no especifica parámetros de fecha (desde/hasta), ni requisitos de autenticación (token/API key), ni límites de rate limiting.** Esto contradice parcialmente la expectativa del barrido previo de que la doc "confirmaría" estos parámetros — no lo hace en el nivel de detalle esperado; probablemente los parámetros de fecha estén documentados en el schema OpenAPI interactivo (Swagger UI) que no se renderizó en el fetch de texto plano, y habría que inspeccionarlo con un cliente HTTP real (`curl https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/USD?...`) en la fase de spike técnico del proyecto.

**Pregunta abierta sin resolver**: límites exactos de rate limiting de BCRA — no se pudo cerrar por agotamiento del presupuesto de búsqueda. Recomendación: validar empíricamente con un script de prueba antes de diseñar el scheduler de ingesta.

## 3. Energía (CAMMESA / datos.energia.gob.ar)

**No se pudo leer esta fuente.** El fetch a `http://datos.energia.gob.ar/dataset/publicaciones-cammesa` (y su variante https) entra en un **bucle de redirección 301** entre el esquema http y https que la herramienta no logra resolver (probablemente el servidor responde con Location apuntando de vuelta al mismo host sin certificado válido, o hay un proxy CDN con configuración inconsistente). Se intentó 3 veces con ambos esquemas sin éxito.

**Consecuencia**: los findings previos sobre CAMMESA (datasets de generación por máquina en MWh vía OData, balance del MEM, consumo de combustible) **no pudieron ser verificados ni corregidos** en esta pasada — se mantienen con el nivel de confianza "medium" original, heredado del barrido anterior, pero **se degrada a "no verificado en esta lectura"**. La pregunta abierta sobre si CAMMESA expone un endpoint OData en vivo o solo CSV periódico **sigue sin resolución**. Esto es relevante porque condiciona una decisión de arquitectura central (batch vs. pseudo-streaming) — ver sección de afirmaciones críticas más abajo.

## 4. Meteorología (Open-Meteo)

Se leyó completa `https://open-meteo.com/en/docs/historical-weather-api`. Esta lectura **corrige un dato importante** del barrido previo:

| Producto | Resolución espacial confirmada | Cobertura temporal |
|---|---|---|
| ERA5 | **0.25° (~25 km)** | 1940 a la actualidad |
| ERA5-Land | **0.1° (~11 km)** | 1950 a la actualidad |
| ECMWF IFS | 9 km | desde 2017, sin delay |

El barrido previo no había afirmado literalmente "hasta 1 km" pero la pregunta abierta sí planteaba esa comparación; con esta lectura queda establecido que la resolución real de Open-Meteo para datos históricos es **11–25 km**, no del orden de 1 km. Esto es comparable, y algo más fino, a la resolución de NASA POWER (0.5°×0.625° ≈ 55×70 km), por lo que **para Vaca Muerta conviene priorizar Open-Meteo (ERA5-Land, ~11 km) como fuente meteorológica principal** y usar NASA POWER solo como fuente secundaria/de respaldo o para variables específicas de radiación solar que Open-Meteo no cubra con el mismo detalle. Esto responde una de las preguntas abiertas del barrido.

Variables confirmadas: horarias (temperatura, humedad, precipitación, viento velocidad/dirección, nubosidad, condiciones de suelo, radiación solar) y diarias agregadas (temp. máx/mín, suma de precipitación, estadísticas de viento, duración de horas de sol). Formatos de salida: JSON (default), CSV y XLSX. Uso no comercial gratuito sin key; uso comercial requiere API key y suscripción paga — esto confirma el hallazgo previo y aclara que el límite de ~10.000 req/día aplicaba al tier gratuito no comercial (no contradicho, pero tampoco re-confirmado explícitamente en el texto de esta página, que habla de "free for non-commercial applications" sin repetir la cifra numérica).

## 5. Info corporativa (SEC EDGAR + YPF IR)

### SEC EDGAR API

Se leyó completa `https://www.sec.gov/search-filings/edgar-application-programming-interfaces`, confirmando de forma precisa 4 endpoints útiles para automatizar la ingesta de los 20-F de YPF **sin scraping manual**:

| API | URL patrón |
|---|---|
| Submissions | `https://data.sec.gov/submissions/CIK##########.json` |
| Company Facts (XBRL) | `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json` |
| Company Concept (XBRL) | `https://data.sec.gov/api/xbrl/companyconcept/CIK##########/us-gaap/[concept].json` |
| Frames (XBRL cross-entity) | `https://data.sec.gov/api/xbrl/frames/us-gaap/[concept]/USD/CY####Q#I.json` |

- **Confirmado textualmente**: *"These APIs do not require any authentication or API keys to access."*
- Respuesta en JSON, actualizada en tiempo real (salvo el bulk nocturno).
- Bulk data disponible como ZIP nocturno (`companyfacts.zip`, `submissions.zip`), actualizado ~3:00 a.m. ET.
- No se documentan límites de rate específicos, pero sí obligación de cumplir la Privacy and Security Policy de SEC.gov (que en la práctica exige un User-Agent identificable, aunque este fetch no lo cita literalmente).

Con CIK `0000904851` de YPF, el pipeline puede usar directamente `https://data.sec.gov/submissions/CIK0000904851.json` para automatizar la detección de nuevos 20-F sin depender del sitio web de EDGAR ni de scraping HTML — esto **reemplaza favorablemente** la necesidad de automatizar `investors.ypf.com`.

### YPF Investor Relations

Se leyó completa `https://investors.ypf.com/financial-information.html`. Confirma que la sección ("Investor Kit") aloja:
- Formulario 20-F (con nota de disponibilidad de copias impresas gratuitas)
- Estados financieros auditados
- Comunicados de resultados trimestrales ("quarterly earnings releases")

**No hay ninguna mención de hojas de datos operativos en Excel descargables** en el contenido accesible del fetch. Esto **respalda, en vez de corregir, el finding previo**: la información operativa (producción por yacimiento, cuenca) no aparece como dataset estructurado en IR, sino previsiblemente incrustada en los PDF/comunicados de resultados trimestrales (el "Report on Form 6-K" o el "Production and Sales Data" que YPF suele adjuntar como PDF a sus resultados trimestrales, aunque esto último no fue confirmado en este fetch específico — solo se advierte que el contenido visible es parcial y probablemente la página tenga más secciones no renderizadas).

**Pregunta abierta sin resolver completamente**: si existen "data sheets" Excel en subsecciones no exploradas del sitio (p. ej. una sección específica de "Operational Data" distinta de "Financial Information") — no se pudo cerrar por agotamiento de presupuesto de búsqueda; requeriría una exploración adicional del sitio (siguiente sesión) o revisión directa del 20-F, que sí trae tablas de producción por yacimiento en formato de texto/tabla dentro del PDF.

## 6. Datasets ML de pozos / sensores (Volve, Sodir)

### Equinor Volve

Se leyó completa `https://www.equinor.com/energy/volve-data-sharing`. **Hallazgo crítico que corrige el barrido previo**: el texto de la página no reproduce los términos exactos de la licencia (remite a un "User Guide" en PDF para el texto completo), pero sí cita textualmente:

> *"all academic institutions, students and researchers permission to use this dataset in accordance with the Equinor Open Data Licence"*

Y that el uso está **"limited to research, study and development purposes"**. Esto es una restricción de alcance de uso **más estrecha** de lo que sugiere el término genérico "dataset abierto" del barrido previo — el texto enfatiza explícitamente instituciones académicas, estudiantes e investigadores, y fines de investigación/estudio/desarrollo, sin mencionar explícitamente "portfolio profesional" o "uso comercial". Esto **no significa necesariamente que un proyecto de portfolio personal esté prohibido** (el Equinor Open Data Licence en su texto completo, ampliamente documentado en la industria como derivado de Norwegian Licence for Open Government Data (NLOD) con cláusulas adicionales, típicamente permite uso, distribución y adaptación con atribución, incluso comercial, según versiones citadas por terceros) — pero **la página oficial en sí no lo aclara** y el fetch no pudo abrir el PDF de la licencia. Se marca como afirmación crítica para verificación adversarial (ver más abajo), porque de ser una restricción real y estricta, condicionaría si el proyecto puede publicar/redistribuir el dataset o solo consumirlo localmente.

No se pudo confirmar en esta lectura el tamaño total del dataset ni el detalle de columnas de la hoja de producción diaria (el fetch declara explícitamente que la página no lo detalla) — estos datos del barrido previo (choke, presión/temperatura de boca de pozo, presión downhole, volúmenes oil/gas/water) **quedan sin verificar en esta pasada**, ni confirmados ni refutados.

### NPD/Sodir FactPages

Se leyó completa `https://factpages.sodir.no/en/wellbore/tableview/exploration/currentyear`. Confirma y **enriquece** el barrido previo con el esquema de columnas real de la tabla de wellbores de exploración:

| Campo | Contenido |
|---|---|
| Wellbore name / NPDID | Identificador único |
| Fechas | Entered, Completed, Last sync con NOD |
| Operador | Ej. Equinor Energy AS, Aker BP ASA, Vår Energi ASA |
| Infraestructura | Production licence, drilling facility |
| Características del pozo | Purpose (wildcat/appraisal), content (oil/gas/dry), status |
| Datos geológicos | Oldest penetrated age, hydrocarbon-bearing age |

**Confirma explícitamente 3 formatos de exportación: EXCEL, XML y CSV** — dato nuevo relevante para el diseño del pipeline (permite descarga estructurada sin scraping). La vista de "currentyear" mostró 23 pozos de exploración/apreciación (ene-ago 2026) en Mar del Norte, Mar de Noruega y Mar de Barents. Este esquema de columnas (nombre, operador, fechas, tipo, licencia, contenido) es un buen candidato de "esquema común" para unir conceptualmente con metadatos de pozos de Vaca Muerta (aunque las coordenadas WGS84 puntuales, citadas en el barrido previo, no aparecieron en esta vista de tabla específica — probablemente estén en la ficha individual de cada wellbore, no en la vista de listado).

## Tabla resumen de correcciones a los findings del barrido previo

| Finding previo | Estado tras esta lectura |
|---|---|
| BCRA expone 3 endpoints (Divisas, Cotizaciones, Cotizaciones/{cod}) | **Confirmado** textualmente |
| EIA API v2 gratuita, requiere registro | **Confirmado**; rutas exactas y límites de rate **no confirmados** en esta página |
| CAMMESA publica datasets OData/CSV en datos.energia.gob.ar | **No verificable** — fuente inaccesible por bucle de redirección |
| Open-Meteo ERA5 desde 1940, ERA5-Land desde 1950 | **Confirmado**, y se agrega resolución exacta: ERA5 0.25° (~25km), ERA5-Land 0.1° (~11km) — corrige la vaga referencia a "hasta 1km" |
| Volve es un dataset "abierto" sin mayores restricciones | **Matizado**: el texto oficial enfatiza uso académico/investigación, no confirma uso comercial/portfolio explícitamente |
| SEC EDGAR tiene filings 20-F de YPF activos | **Confirmado**, y se agrega que existe API JSON sin autenticación (submissions, companyfacts) que reemplaza la necesidad de scraping |
| YPF IR no muestra data sheets Excel operativos | **Confirmado** (no se detectaron en la sección Financial Information) |
| Sodir FactPages permite exportar wellbores | **Confirmado y ampliado**: exportación en EXCEL, XML y CSV; columnas de la vista listadas arriba |

## Preguntas abiertas: estado final

| Pregunta | Estado |
|---|---|
| Límites de rate de EIA API v2 | **Sin resolver** (no documentado en la página leída; presupuesto de búsqueda agotado) |
| Límites de rate de BCRA API | **Sin resolver** (no documentado en el OpenAPI leído) |
| ¿API REST oficial de INDEC para IPC? | **Sin resolver** — no se pudo re-buscar; se mantiene el hallazgo previo (CSV descargable, sin API dedicada confirmada) |
| ¿CAMMESA expone OData en vivo? | **Sin resolver** — fuente inaccesible en esta sesión |
| ¿ENARGAS tiene series históricas de volumen más allá de PDFs? | **Sin resolver** — no se pudo re-buscar |
| ¿YPF publica Excel operativos en alguna subsección de IR? | **Parcialmente resuelto**: no se detectan en la sección principal; subsecciones no exploradas |
| ¿Granularidad geográfica de CNV/AIF para cruzar con pozos? | **Sin resolver** — no se pudo re-buscar |
| Resolución NASA POWER vs. Open-Meteo para Vaca Muerta | **Resuelto**: Open-Meteo (ERA5-Land ~11km) es más fino que NASA POWER (~55×70km) y debe priorizarse como fuente meteorológica principal |

## Afirmaciones críticas para verificación adversarial

Se seleccionan dos afirmaciones cuyo error dañaría más el diseño del proyecto, detalladas en el campo `critical_claims` de la salida estructurada:

1. **Licencia de uso del dataset Equinor Volve** — si la restricción a "academic institutions, students and researchers" para fines de "research, study and development" es una limitación legal real que excluye un portfolio profesional no académico, todo el bloque 6 (ML de pozos/sensores) del proyecto debería reconsiderar su fuente principal o buscar una vía de uso compatible (ej. encuadrar el proyecto explícitamente como estudio/aprendizaje personal).
2. **Accesibilidad real de CAMMESA vía datos.energia.gob.ar** — la fuente resultó completamente inaccesible en esta sesión (bucle de redirección), lo cual es una señal de alarma sobre la estabilidad del portal como fuente productiva; si el portal está caído, migrado o con problemas de infraestructura persistentes, la arquitectura de ingesta de datos de energía eléctrica (proxy de demanda industrial correlacionable con actividad de YPF) necesita una fuente alternativa o un método de verificación distinto (p. ej. mirror en Github, Kaggle, o consulta directa a la API pública de CAMMESA en `cammesaweb.cammesa.com` en vez de datos.energia.gob.ar).

## Fuentes

- https://www.eia.gov/opendata/
- https://estadisticas-cambiarias.bcra.apidocs.ar/
- http://datos.energia.gob.ar/dataset/publicaciones-cammesa (inaccesible — bucle de redirección http/https)
- https://open-meteo.com/en/docs/historical-weather-api
- https://www.equinor.com/energy/volve-data-sharing
- https://www.sec.gov/search-filings/edgar-application-programming-interfaces
- https://investors.ypf.com/financial-information.html
- https://factpages.sodir.no/en/wellbore/tableview/exploration/currentyear

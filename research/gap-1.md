# Gap 1 — Fuentes de datos para el módulo de streaming / ML de series temporales

**Objetivo:** cerrar el gap de datos.energia.gob.ar (mensual) para un proyecto portfolio tipo "RTIC de YPF" que necesita telemetría de sensores de pozo a alta frecuencia, corriendo en AWS Free Tier / Databricks Free Edition / Docker local.

**Método:** verificación con fuentes primarias vía `gh api` (GitHub), `curl` sobre raw.githubusercontent.com, y WebFetch sobre equinor.com, NASA, GitHub topics. El presupuesto de WebSearch de la sesión se agotó (200/200) antes de empezar esta tarea, así que **no pude ejecutar búsquedas nuevas**; todo lo reportado sale de fetches directos a URLs conocidas o descubiertas por enlaces dentro de esas páginas. Donde no logré verificar algo lo marco explícitamente como "no verificado".

---

## 1. Dataset 3W de Petrobras — VERIFICADO en profundidad

Fuente leída directamente: [README.md](https://github.com/petrobras/3W/blob/main/README.md), [dataset/README.md](https://github.com/petrobras/3W/blob/main/dataset/README.md), [dataset/dataset.ini](https://github.com/petrobras/3W/blob/main/dataset/dataset.ini), [CITATION.md](https://github.com/petrobras/3W/blob/main/CITATION.md), y listado real de archivos vía GitHub API (`gh api repos/petrobras/3W/contents/dataset/*`).

- **Licencia:** dos licencias distintas y confirmadas textualmente en el README: código bajo **Apache 2.0 License**; "all 3W Dataset's data files (Parquet files saved in subdirectories of the dataset directory) are licensed under the **Creative Commons Attribution 4.0 International License**" (CC BY 4.0). CC BY 4.0 permite uso comercial y en portfolio profesional citando la fuente — es la licencia más permisiva posible para este caso. **Apto para portfolio: SÍ**, con atribución (cita provista en CITATION.md).
- **Tamaño total:** el propio changelog (dataset/README.md) declara que la v2.0.0 (julio 2024) redujo el dataset de 4.89 GB a **1.74 GB** (parquet + compresión brotli). Medición independiente que hice sumando el tamaño real de todos los `.parquet` en las 10 carpetas de clase vía API dio **~1.87 GB** — consistente en orden de magnitud (la diferencia puede deberse a archivos añadidos después del changelog citado).
- **Formato:** Parquet (motor `pyarrow`, compresión `brotli`), confirmado en `dataset.ini`.
- **Estructura y conteo real (verificado archivo por archivo vía API, no estimado):**

| Clase | Nombre (dataset.ini) | Instancias reales | Simuladas | Hand-drawn | Total archivos | Tamaño (MB) |
|---|---|---|---|---|---|---|
| 0 | NORMAL | 594 | 0 | 0 | 594 | 162 |
| 1 | ABRUPT_INCREASE_OF_BSW | 4 | 114 | 10 | 128 | 230 |
| 2 | SPURIOUS_CLOSURE_OF_DHSV | 22 | 16 | 0 | 38 | 18.5 |
| 3 | SEVERE_SLUGGING | 32 | 74 | 0 | 106 | 187.5 |
| 4 | FLOW_INSTABILITY | 343 | 0 | 0 | 343 | 63 |
| 5 | RAPID_PRODUCTIVITY_LOSS | 11 | 439 | 0 | 450 | 420.5 |
| 6 | QUICK_RESTRICTION_IN_PCK | 6 | 215 | 0 | 221 | 168 |
| 7 | SCALING_IN_PCK | 36 | 0 | 10 | 46 | 137 |
| 8 | HYDRATE_IN_PRODUCTION_LINE | 14 | 81 | 0 | 95 | 152.7 |
| 9 | HYDRATE_IN_SERVICE_LINE | 57 | 150 | 0 | 207 | 332.8 |

  (Nota: falta clase "TRANSIENT_OFFSET" documentada como offset de etiqueta, no un tipo de evento adicional. `EXTRA_INSTANCES_TRAINING` en `dataset.ini` es solo un flag interno.)

- **9 clases de eventos anómalos + 1 normal (0-9), confirmadas textualmente en `dataset.ini`:** NORMAL(0), ABRUPT_INCREASE_OF_BSW(1), SPURIOUS_CLOSURE_OF_DHSV(2), SEVERE_SLUGGING(3), FLOW_INSTABILITY(4), RAPID_PRODUCTIVITY_LOSS(5), QUICK_RESTRICTION_IN_PCK(6), SCALING_IN_PCK(7), HYDRATE_IN_PRODUCTION_LINE(8), HYDRATE_IN_SERVICE_LINE(9).
- **27 variables/columnas** (v2.0.0, confirmado en `dataset.ini`, con descripción de cada tag): `timestamp`, `ABER-CKGL`, `ABER-CKP`, `ESTADO-DHSV`, `ESTADO-M1`, `ESTADO-M2`, `ESTADO-PXO`, `ESTADO-SDV-GL`, `ESTADO-SDV-P`, `ESTADO-W1`, `ESTADO-W2`, `ESTADO-XO`, `P-ANULAR`, `P-JUS-BS`, `P-JUS-CKGL`, `P-JUS-CKP`, `P-MON-CKGL`, `P-MON-CKP`, `P-MON-SDV-P`, `P-PDG`, `PT-P`, `P-TPT`, `QBS`, `QGL`, `T-JUS-CKP`, `T-MON-CKP`, `T-PDG`, `T-TPT`, más `class` (etiqueta) y `state` (estado operacional del pozo). (v1 tenía `T-JUS-CKGL`, removida en v2; se agregaron 20 variables nuevas respecto a v1).
- **Frecuencia de muestreo:** el README/dataset.ini **no especifica explícitamente un valor en Hz** en el texto que pude leer — solo aparece un campo `WINDOW`/`STEP` (en segundos, ej. clase 2: WINDOW=180, STEP=15) usado por el toolkit para ventaneo, no la frecuencia de muestreo cruda del sensor. **No pude verificar el valor exacto de Hz/segundos entre samples** desde las fuentes leídas; es ampliamente citado en literatura externa como ~1 muestra/segundo, pero como no lo leí en el texto oficial, lo marco como **no verificado en esta sesión**.
- **Papers verificados** (citados textualmente en CITATION.md): v1 — *"A realistic and public dataset with rare undesirable real events in oil wells"*, Journal of Petroleum Science and Engineering, 181, DOI [10.1016/j.petrol.2019.106223](https://doi.org/10.1016/j.petrol.2019.106223) (2019); v2 — *"3W Dataset 2.0.0: a realistic and public dataset with rare undesirable real events in oil wells"*, Scientific Data 13, 949, DOI [10.1038/s41597-026-07225-z](https://doi.org/10.1038/s41597-026-07225-z) (2026).
- **Subset mínimo usable:** clase 0 (162 MB, 594 instancias reales normales) + clase 2 (18.5 MB) + clase 7 (137 MB) ≈ **~318 MB**, muy por debajo de 5 GB. Alternativa aún más chica: solo clase 2 (18.5 MB, incluye reales y simuladas) para un prototipo de detección binaria normal/anómalo.

## 2. Equinor Volve — AMBIGÜEDAD CERRADA (parcialmente)

- **Ruta de descarga hoy (2026):** confirmado por fetch directo — `data.equinor.com/dataset/Volve` devuelve **301 redirect** a `https://www.equinor.com/energy/data-sharing`, y esta a su vez apunta a **Databricks Marketplace** como único canal de acceso. El PDF oficial *"How to Access and Use Equinor Open Data in the Databricks Marketplace"* (`https://equinoropendata.blob.core.windows.net/userguides/Equinor%20open%20data%20-%20User%20Guide.pdf`, leído completo) confirma: se accede vía botón "Get instant access" dentro de Databricks Marketplace, con `Pricing: Free`, `Access: Instantly available`, `Categories: Education`, `Visibility: Public`. **No es una descarga de archivo plano** (ZIP/CSV/XLSX) sino un producto de datos (Delta Sharing / catálogo Unity Catalog) que aparece en tu Catálogo Databricks hasta 1 hora después de solicitarlo. Requiere cuenta Databricks (login/sign-up con cuenta universitaria o de empresa).
- El antiguo acceso vía Azure Blob directo / FTP que se documentaba en años previos **ya no es la ruta oficial** — el propio dominio `equinoropendata.blob.core.windows.net` solo aloja hoy la guía PDF, no until confirmé los datos en sí.
- **Tamaño:** el anuncio oficial de 2018 (`equinor.com/news/archive/14jun2018-disclosing-volve-data`, leído) dice **"around 40,000 files"** cubriendo modelos estáticos/dinámicos, datos de pozo y perforación en tiempo real, producción, geofísica e informes técnicos — **sin especificar TB/GB**. No pude confirmar el tamaño total en TB citado habitualmente en blogs (decenas de TB), porque las páginas que leí no lo mencionan.
- **Licencia:** confirmé que existe y se llama **"Equinor Open Data Licence"**, referenciada en la página de data-sharing, pero **no pude acceder al texto completo de la licencia** — los intentos de fetch a rutas plausibles de PDF devolvieron 404, y no pude usar WebSearch (presupuesto agotado) para localizar un espejo o copia en archive.org (que además está bloqueado para WebFetch en este entorno). **Este punto queda explícitamente NO VERIFICADO**: no cito cláusulas porque no leí el documento.
- **Subset chico:** no pude confirmar la existencia de un subset ligero (XLSX de producción mensual, LAS de well logs) descargable de forma independiente del Marketplace — las rutas antiguas que solían servir eso ya no resuelven. **Apto para portfolio: AMBIGUO** — el dato es legítimo y gratuito, pero el mecanismo de acceso (Databricks Marketplace/Delta Sharing) no encaja con "5 GB en S3 / Docker local" sin pasos adicionales de export, y la licencia exacta no quedó verificada.

## 3. Alternativas de respaldo

| Fuente | Estado de verificación |
|---|---|
| **NASA C-MAPSS** (turbofan degradation) | Verificación **contradictoria** entre dos páginas oficiales de NASA: la página del PCoE (`nasa.gov/intelligent-systems-division/.../pcoe-data-set-repository`) dice que está disponible como ZIP vía un enlace S3, con términos "users employ the data at their own risk" y pide atribución a NASA y a los donantes; la página `data.nasa.gov/dataset/c-mapss-aircraft-engine-simulator-data` dice literalmente **"C-MAPSS and C-MAPSS40K ARE CURRENTLY UNAVAILABLE FOR DOWNLOAD"** y lista licencia como "License not specified". Confirmado: 30 parámetros de motor/vuelo, muestreo a 1 Hz, vuelos de ~90 min, fallas inyectadas en fan/compresores/turbina. Es un dominio análogo (mantenimiento predictivo industrial), no oil&gas. **No pude verificar tamaño en MB ni si el ZIP realmente descarga hoy.** Apto para portfolio: **ambiguo** (licencia poco clara + disponibilidad en duda ahora mismo).
| **Sodir/NPD Diskos** | `diskos.no` redirige (301) a `sodir.no/en/diskos/`, que devolvió **403 Forbidden** al fetch — no pude leer su contenido. No verificado en esta sesión.
| **Mendeley Data (ESP/SCADA)** | El dataset específico que intenté abrir devolvió "Dataset Not Found". La búsqueda por palabra clave en el sitio no fue accesible sin WebSearch. No verificado.
| **Kaggle (oil well sensor / ESP / SCADA)** | La página de resultados de búsqueda de Kaggle es renderizada por JavaScript y WebFetch solo devolvió el shell vacío ("Search | Kaggle"), sin listado real. No pude verificar ningún dataset concreto de Kaggle en esta sesión — no voy a inventar URLs.
| **UCI ML Repository** | No pude ejecutar búsqueda (WebSearch agotado) ni tenía una URL directa conocida para intentar fetch dirigido. No verificado.

Estas alternativas quedan como **pendientes de verificación** — no las descarto, pero tampoco las recomiendo sin confirmar tamaño/licencia con una sesión que tenga presupuesto de búsqueda disponible.

## 4. Generador sintético — VERIFICADO (existencia y licencia, no la calidad técnica)

Vía GitHub topic `witsml` y `gh search repos`, confirmé la existencia de:

- **[`SyntheticFunk/drilling-telemetry-simulator`](https://github.com/SyntheticFunk/drilling-telemetry-simulator)** — "Physics-based surface-to-surface drilling rig simulator for generating synthetic EDR telemetry and directional well data", en Python, **licencia Apache 2.0** (confirmada vía API de GitHub), 2 stars. Es un proyecto pequeño y poco usado (bajo mantenimiento probable), pero de licencia permisiva y con propósito exacto (generar telemetría sintética de perforación que podría alimentar un productor Kafka).
- También aparecen en el mismo topic: `welleng` (160 stars, trayectoria de pozo + WITSML), `fesapi` (44 stars, estándares Energistics/WITSML/RESQML/ProdML), `komle` (38 stars, librería Python para WITSML), `witsml21parser`, `witsml-converter`, `witsml-consumer-api` — ninguno de ellos es en sí mismo un simulador de telemetría continua, son parsers/convertidores de formato WITSML.

**Evaluación honesta:** un generador sintético (aunque sea de bajo mantenimiento como `drilling-telemetry-simulator`) es **más defendible ante un reclutador de YPF** que usar Volve (Noruega, mecanismo de acceso ambiguo) o C-MAPSS (motores de avión, dominio distinto), PERO es menos defendible que usar 3W real de Petrobras, que es datos reales de pozos de petróleo con etiquetas de expertos y paper revisado por pares.

---

## Tabla comparativa final

| Fuente | URL de descarga verificada | Tamaño subset mínimo | Formato | Frecuencia | Nº señales | Licencia (cita textual) | ¿Apto portfolio? |
|---|---|---|---|---|---|---|---|
| **3W Petrobras** | [github.com/petrobras/3W](https://github.com/petrobras/3W) (dataset/0, dataset/2, dataset/7) | ~318 MB (o 18.5 MB solo clase 2) | Parquet (pyarrow+brotli) | No verificada en texto oficial (comúnmente citada como 1 Hz, sin confirmar aquí) | 27 | *"licensed under the Creative Commons Attribution 4.0 International License"* | **SÍ** |
| **Equinor Volve** | [equinor.com/energy/volve-data-sharing](https://www.equinor.com/energy/volve-data-sharing) → Databricks Marketplace | No determinado (no es archivo plano; ~40.000 archivos totales, sin GB confirmado) | Multi-formato (según Marketplace) | N/D | N/D | *"Equinor Open Data Licence"* (nombre confirmado, texto NO leído) | **AMBIGUO** |
| NASA C-MAPSS | nasa.gov PCoE (dice disponible) vs. data.nasa.gov (dice "UNAVAILABLE FOR DOWNLOAD") — **contradicción no resuelta** | No verificado | ZIP (según PCoE) | 1 Hz (confirmado) | 30 | "License not specified" (data.nasa.gov) | **AMBIGUO** |
| Sodir/Diskos | sodir.no/en/diskos/ (403 al fetch) | No verificado | No verificado | No verificado | No verificado | No verificado | **NO VERIFICADO** |
| Mendeley ESP/SCADA | intento de URL específica → 404 | No verificado | No verificado | No verificado | No verificado | No verificado | **NO VERIFICADO** |
| Kaggle oil/ESP/SCADA | búsqueda no renderizable vía WebFetch | No verificado | No verificado | No verificado | No verificado | No verificado | **NO VERIFICADO** |
| Generador sintético WITSML | [github.com/SyntheticFunk/drilling-telemetry-simulator](https://github.com/SyntheticFunk/drilling-telemetry-simulator) | N/A (genera on-demand) | Código Python, salida configurable | Configurable | Configurable | Apache 2.0 | **SÍ** (como productor Kafka) |

## Recomendación

**Usar el dataset 3W de Petrobras como fuente para el módulo de streaming**, tomando el subset de las clases 0, 2 y 7 (~318 MB, muy por debajo del límite de 5 GB de S3 free tier, y trivialmente cargable en Docker local). Razones, basadas en lo verificado:

1. Es el único de los candidatos con **licencia leída textualmente y sin ambigüedad** (CC BY 4.0 para los datos, Apache 2.0 para el código del toolkit) — apto para portfolio profesional con solo citar la fuente.
2. Es **oil & gas real**, con eventos anómalos etiquetados por expertos de Petrobras y respaldado por un paper en Scientific Data (Nature) — argumento fuerte frente a un reclutador de YPF, mucho más que datos de motores de avión (C-MAPSS) o de un campo noruego con mecanismo de acceso poco claro (Volve).
3. Tamaño verificado archivo-por-archivo (no estimado): el subset propuesto cabe cómodo en cualquiera de los tres entornos objetivo (S3 free tier, Databricks Free Edition, Docker local con RAM limitada).
4. Formato Parquet ya está listo para ingestión batch/replay (podés leer fila a fila y "reproducirlo" cronológicamente hacia un tópico Kafka simulando streaming, algo que no se puede hacer con la fuente mensual de datos.energia.gob.ar).

Como complemento — no como reemplazo — considerar el **`drilling-telemetry-simulator`** (Apache 2.0) para generar telemetría *adicional* de perforación con más variables o mayor frecuencia si 27 columnas y el subset de 3W quedan cortos para "+60 variables"; documentando explícitamente en el README del portfolio que esa porción es sintética y por qué (honestidad defendible ante un entrevistador técnico).

**Lo que NO recomiendo:** apoyarse en Volve como fuente principal del módulo de streaming, dado que (a) no pude confirmar que exista hoy un subset descargable como archivo plano fuera de Databricks Marketplace, y (b) la licencia exacta no quedó verificada en esta sesión — usarlo sin leer esa licencia es un riesgo para un proyecto que se quiere mostrar a reclutadores.

**Lo que quedó sin verificar y requeriría una sesión con presupuesto de WebSearch disponible:** el texto completo de la Equinor Open Data Licence, cualquier dataset concreto de Kaggle/UCI/Mendeley, y el estado real de acceso a Sodir/Diskos y a C-MAPSS (dado el mensaje contradictorio "unavailable for download" en una de las dos páginas oficiales de NASA).

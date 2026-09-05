# Informe consolidado — Proyecto portfolio "YPF Data Platform"

**Fecha de consolidación:** 2026-09-04
**Fuente:** síntesis de 11 informes de investigación (`ypf-digital.md`, `ypf-jobs.md`, `datasets-energia.md`, `datasets-complementarios.md`, `aws-free-tier.md`, `databricks-free-y-saas.md`, `og-ml-arquitecturas.md`, `gap-1.md` a `gap-4.md`) más una tabla de verificaciones adversariales que corrige seis afirmaciones previas.
**Convención de marcado usada en todo el documento:**
- **[V]** = verificado con fuente primaria leída directamente (cita textual o medición empírica).
- **[P]** = parcialmente verificado / con matiz.
- **[?]** = incierto, no verificado o no documentado públicamente.
- **[X]** = refutado por verificación adversarial (ver sección 13).

Nada de lo que sigue introduce datos o URLs que no estén respaldados por alguno de los 11 informes. Cuando algo no se pudo verificar, se dice explícitamente en vez de rellenarlo.

---

## 1. Resumen ejecutivo

La investigación deja el proyecto en una posición asimétrica: **la narrativa de negocio y la fuente batch núcleo están sólidas y verificadas; la infraestructura gratuita y la fuente de alta frecuencia estaban abiertas y hoy quedan cerradas, pero con condiciones que cambian la arquitectura respecto de lo que se venía asumiendo.**

**Lo que está firme.** YPF tiene un caso emblemático y bien documentado por prensa: una red de *Real Time Intelligence Centers* (RTIC) y una *Real Time Operations Room* (RTOR) que monitorean perforación, producción, logística y estaciones de servicio. Las cifras verificadas son la mejor guía de dimensionamiento que existe: 35 millones de datos por pozo, 80–100 variables y 80+ KPIs por equipo, 13 equipos de perforación en simultáneo (16 es capacidad de diseño, no operación real **[X]**), 88 profesionales en turnos 7×7 y Starlink en el RTIC de Puerto Madero; 2.000+ pozos, 1,5 millones de variables y 13 drones en el de Neuquén; 1.600+ estaciones y 2.400 camiones geolocalizados en el de Comercialización, con micropricing y 97% de correlación entre conteo vehicular y ventas. Del lado de datos abiertos, el dataset núcleo (`produccion-de-petroleo-y-gas-por-pozo`) está verificado hasta el byte: 53 recursos, esquema de 38 columnas, CSV 2006 de 235.915.154 bytes con 748.653 filas, de las cuales 370.449 (~49%) son de `YPF S.A.` **[V]**.

**Lo que cambió con las verificaciones finales.** Cuatro decisiones de arquitectura que estaban tomadas a ciegas ahora tienen respuesta:

1. **Existe una fuente real de alta frecuencia apta para portfolio.** El **3W de Petrobras** (GitHub, Parquet, 27 variables, 10 clases de eventos de pozo etiquetados por expertos, paper en *Scientific Data*) tiene licencia **CC BY 4.0 para los datos** y Apache 2.0 para el código, leída textualmente **[V]**. El subset de clases 0+2+7 pesa ~318 MB; solo la clase 2, 18,5 MB. Reemplaza a Volve como fuente del módulo streaming: Volve hoy solo se accede vía Databricks Marketplace/Delta Sharing y su licencia completa no se pudo leer **[?]**, aunque la guía oficial vigente sí contempla uso "student, researcher, or professional" **[X]** — la supuesta restricción "solo académicos" era una lectura incorrecta.
2. **Databricks Free Edition no puede ser la capa de ingesta.** La restricción de egress (*"outbound internet access is restricted to a limited set of trusted domains"*) está **confirmada oficialmente** **[V]**, la allowlist no es pública **[?]**, y el portal argentino fuerza HTTP plano. Topología correcta: ingesta fuera de Databricks (GitHub Actions o Docker local) → Volume de Unity Catalog → transformación/serving en Databricks. La verificación por LinkedIn amplía el egress y es la mitigación a probar antes de descartar Asset Bundles **[P]**.
3. **AWS no puede sostener el portfolio "gratis para siempre".** Cita oficial: *"After your free account plan expires, your account closes automatically, and you lose access to your resources and data"*, con 90 días de gracia y borrado definitivo **[V]**. No hay modo "solo Always Free" indefinido dentro del Free account plan. La vía sostenible es hacer *upgrade* a Paid plan y operar dentro de cuotas Always Free verificadas (Lambda, DynamoDB, Step Functions, SQS, CloudWatch, EventBridge, Glue Data Catalog): **USD 1–5/mes** estimados (estimación propia **[?]**).
4. **El stack de YPF sí tiene una pieza auditable, y no es la que se suponía.** El Form 20-F FY2025: *"During 2025, we completed the implementation of the system SAP S/4 Hana Solutions, to replace the commercial and stock systems related to the Downstream business segment"* **[V]**. En cambio "Microsoft", "Azure", "Amazon", "AWS", "cloud", "Databricks", "Snowflake", "Kafka", "Spark", "Palantir", "Globant" y "Corva" tienen **cero apariciones** en ese documento **[V, negativo]**, y Microsoft Customer Stories devuelve *"0 results"* para YPF **[V, negativo]**.

**Consecuencia estratégica.** El proyecto no debe venderse como réplica del stack de YPF (no es público) sino como **una plataforma de datos de dominio Oil & Gas sobre datos reales argentinos, dimensionada con las cifras públicas del RTIC, con un extractor tipo ERP-SAP y un módulo de telemetría de pozo con datos reales de Petrobras**. Esa formulación es defendible ante alguien que conozca la casa por dentro, y el rigor de la investigación (decir qué se verificó y qué no) es en sí una señal de seniority.

**La arquitectura recomendada, en una línea:** híbrido — *ingesta y desarrollo en Docker local (Spark + MinIO + Iceberg/Delta + Kafka), publicación de las capas gold en Databricks Free Edition vía Volumes para el "shop window" (Unity Catalog + SQL Warehouse + AI/BI), y un despliegue AWS acotado y efímero (S3 + Lambda + Step Functions + Glue Data Catalog + Athena) para demostrar cloud-nativo dentro de la ventana de crédito de 6 meses*, con IaC en Terraform, CI en GitHub Actions (gratis e ilimitado en repos públicos **[V]**) y observabilidad en Grafana Cloud Free.

---

## 2. Problemas y desafíos de datos de YPF (con evidencia y fuentes)

### 2.1 Volumen y velocidad en upstream: el problema del RTIC

El desafío central que YPF comunica públicamente es **convertir telemetría de perforación y producción, dispersa en el desierto neuquino, en decisiones operativas en minutos**. Evidencia verificada:

| Centro | Inauguración | Alcance | Datos/variables | Personal |
|---|---|---|---|---|
| RTIC Upstream (piso 26, Torre YPF) | 13-dic-2024 **[V]** | **13 equipos operando**; 16 = capacidad de diseño **[X]**; meta 200–210 pozos/año | 35 M datos/pozo; 100+ variables y 80+ KPIs; 4 equipos por puesto | 88 profesionales, turnos 7 h en 7×7 |
| RTIC Comercialización (piso 11) | No confirmado **[?]** | 1.600+ estaciones, 2.400 camiones | Datos por surtidor; conteo vehicular con 97% de correlación con ventas | No especificado |
| RTIC Upstream Neuquén | Agosto 2025 **[V]** | 2.000+ pozos, 100+ instalaciones, 290 camiones, 8 equipos de pulling, >90 MW | 1,5 M variables en tiempo real; 150+ cámaras, 13 drones | 129 personas, 54 puestos |
| RTOR La Plata | **23-dic-2025** **[X]** (no marzo) | Refinería >210.000 bbl/día, 70% shale de Vaca Muerta | **No confirmado**: "200.000 variables" y "+20% de rentabilidad" refutados **[X]** | No especificado |

Detalles adicionales verificados del RTIC de Puerto Madero: 130 pantallas en 350 m², construcción en 4 meses y medio, perforación en tres tramos (superficial ≤800 m, intermedio 800–2.300 m, producción con récord de 8.300 m), laterales de hasta 5.170 m, caso PAD 346 de Loma Campana con 6 pozos y 342 etapas de fractura, **más de 60 variables en tiempo real** por equipo, **8 sets de fractura simultáneos** y un modelo predictivo entrenado con **más de mil pozos perforados** históricos **[V]**. Trazabilidad: la cifra "2.000+ pozos" **no** aparece en ese artículo **[X]**, sino en otro distinto sobre el **RTIC de Neuquén**; no mezclar fuentes.

**Traducción a desafíos de ingeniería:** (a) series temporales multi-fuente con conectividad intermitente (Starlink) → *late-arriving data*, idempotencia y *watermarks*; (b) dos naturalezas de dato conviviendo — variables crudas vs. KPIs calculados, lo que sugiere modelarlas como `raw_variables` y `computed_kpis` separadas en silver **[P]**; (c) contexto operacional: un dato de sensor sin pozo, pad, equipo y etapa de fractura no vale nada → modelo dimensional con SCD sobre entidades de pozo.

### 2.2 Downstream: pricing, demanda y logística

El RTIC de Comercialización expone un problema clásico de *demand forecasting* + *revenue management*: micropricing sobre indicadores macroeconómicos, inflación, tipo de cambio, demanda, franjas horarias, precios de competidores y diferenciales, con resultados declarados de **+35% de rentabilidad nocturna** (junio–julio) y reducción del tiempo de carga de 5 a menos de 3 minutos en tres meses **[V]**. El software de conteo vehicular fue desarrollado internamente por la Gerencia de Tecnología de YPF **[V]**.

Es el bloque más fácil de reproducir con datos abiertos reales: **Precios en Surtidor (Res. 314/2016)** aporta precio por estación, bandera y producto; **Precios y Volúmenes EESS (Res. 1104/04)**, volúmenes vendidos. Con FX del BCRA y precios de EIA se arma un caso de micropricing/elasticidad *sin inventar datos*.

### 2.3 Fragmentación, gobernanza y coexistencia IT/OT

"Digital Suppl.AI" (YPF–Globant) confirma **46 agentes de IA en 8 soluciones agénticas** **[V]** sobre compras, inventario, contratos y proveedores, y nombra el problema de fondo: trazabilidad punta a punta sobre **"datos fragmentados"**. AWS, OpenAI, NVIDIA y Unity aparecen como partners **de Globant**, no como stack contratado por YPF **[V, matizado]**.

El Form 20-F FY2025 aporta lo único auditable: **SAP S/4HANA implementado en 2025 para Downstream** **[V]**; **YPF Digital S.A.U.** como subsidiaria de desarrollo digital (sin detalle de stack **[?]**); un **CISO** desde enero 2023 con trayectoria en *"control and telemetry systems... IT, OT, Cybersecurity and data architecture"*; un **SOC OT**; y adopción genérica de IA *"available via open source or commercial license agreements"* **[V]**. **Desafío derivado:** la coexistencia IT/OT (ERP + historiadores de planta + telemetría de campo) es justamente lo que resuelve un lakehouse con Unity Catalog / Glue Data Catalog — linaje, control de acceso por dominio y contratos de datos entre sistemas con dueños distintos.

### 2.4 Opacidad arquitectónica: el riesgo de diseño más importante

No existe blog de ingeniería, paper ni whitepaper de YPF que describa la arquitectura del RTIC **[V, negativo]**. Las únicas piezas con anclaje público: **Azure OpenAI Service** para GAIA/Y-Click! (acuerdo del **28-ago-2024**, prototipo funcional en tres meses según Leandro Masciotta) **[V]**; **Corva** (Houston) como "sistema operativo digital para la construcción de pozos", con renovación **anunciada el 1-sep-2026** — no consta fecha de firma **[X]**; **Nova** y **Argus**, ambos descritos como desarrollo interno **[V]**; y **SAP S/4HANA** vía 20-F. Todo lo demás es inspiración, no réplica.

---

## 3. Qué pide YPF en sus búsquedas laborales (ranking con citas)

**Advertencia metodológica que hay que sostener en la entrevista:** no se encontró ningún aviso de YPF que nombre textualmente Databricks, Snowflake, Kafka, Spark, dbt, MLflow, Palantir, Dataiku o SAP BW **[V, negativo]**. El ranking siguiente ordena por **calidad de la evidencia**, no por frecuencia estadística — no hay base para un ranking por frecuencia.

| # | Tecnología / competencia | Evidencia textual | Fuente | Confianza |
|---|---|---|---|---|
| 1 | **SAP / SAP S/4HANA** | *"we completed the implementation of the system SAP S/4 Hana Solutions, to replace the commercial and stock systems related to the Downstream business segment"* (FY2025); *"...application/database/SAP Basis maintenance..."* (bio del CISO) | Form 20-F SEC, 2026-03-26 | **Alta [V]** |
| 2 | **IA / agentes de IA** | 46 agentes en 8 soluciones agénticas (Digital Suppl.AI); *"adopting AI technology available via open source or commercial license agreements"* (20-F) | PRNewswire 29-oct-2025; 20-F | **Alta** (existencia) / nula (stack) |
| 3 | **"Tecnología / Datos / IA" como bloque** | *"2 años de experiencia comprobable en Tecnología / Datos / IA"*; reporta a la "vicepresidencia de tecnología de YPF"; 10 vacantes (7 Buenos Aires, 3 Neuquén); inglés avanzado; híbrido | Programa de Jóvenes en Tecnología (HiringRoom) | **Alta [V]** |
| 4 | **Analítica en tiempo real / series temporales de pozo** | *"modelo de operación en tiempo real, basado en datos, analítica avanzada e inteligencia artificial"*; *"seguir el desempeño de los pozos, anticipar potenciales eventos de riesgo"* | Río Negro (YPF–Corva) | **Alta** (concepto) |
| 5 | **Azure / Azure OpenAI** | GAIA sobre Azure OpenAI Service integrado a Y-Click! | ADN Sur, 28-ago-2024 | **Media-alta** (acotado a apps, no a infraestructura corporativa) |
| 6 | **Power BI** | Valorado (no excluyente) en un aviso de analista de gestión | Veintitrés 2023 (citado de barrido previo) | **Media [?]** (aviso antiguo, no de datos puro) |
| 7 | **AWS** | Partner de Globant; alianza de migración mencionada en prensa no re-verificada | PRNewswire; iProfesional | **Media [?]** — cero menciones en el 20-F |
| 8 | **Python, SQL, PowerShell, R, Data Warehouse, Data Lake** | Aviso 2023 vía Bumeran | Bumeran (fetch devolvió página vacía) | **Muy baja / no verificado [?]** |

**Contexto salarial verificado** (El Cronista, fecha real **31-dic-2024**, no marzo 2025 **[X]**): Tecnología/Programación junior ~ARS 2.000.000; senior/semi-senior ARS 4.000.000–5.000.000 mensuales **[V]**. El desglose por especialidades ("Ciberseguridad, Cloud, IA, ML, Soporte, Análisis de Negocio y Datos") no se pudo reproducir textualmente en la relectura y baja a confianza media **[?]**.

**Cómo usar esto en el portfolio.** El README debe decir explícitamente que el stack se eligió por relevancia de mercado y por las dos únicas anclas públicas (SAP como fuente transaccional; Azure/Power BI en la capa de apps y BI), no por un requisito textual de YPF. Un extractor "tipo SAP → lake" y una capa de BI son las dos piezas con mejor justificación empírica de todo el diseño.

---

## 4. Inventario de datasets reales

### 4.1 Tabla maestra

| Dataset | URL | Formato | Tamaño / filas | Columnas clave | Frecuencia | Rango | Filtro YPF | Verificado |
|---|---|---|---|---|---|---|---|---|
| **Producción de petróleo y gas por pozo (Cap. IV)** — 53 recursos | `http://datos.energia.gob.ar/api/3/action/package_show?id=produccion-de-petroleo-y-gas-por-pozo` (pkg `c846e79c-026c-4040-897f-1ad3543b407c`) | CSV (+1 SHP) | CSV 2006: **235.915.154 B, 748.653 filas**; 2024: 319.898.240 B | `idempresa, anio, mes, idpozo, prod_pet, prod_gas, prod_agua, iny_agua/gas/co2/otro, tef, vida_util, tipoextraccion, tipoestado, tipopozo, empresa, sigla, formacion, areayacimiento, cuenca, provincia, tipo_de_recurso, sub_tipo_recurso` (38 col.) | **Mensual por pozo** | 2006–2026 | `empresa == 'YPF S.A.'` → **370.449 / 748.653 filas en 2006 (~49%)** | **Sí [V]** (descarga completa + `wc -l` + `awk`) |
| Recursos hermanos del mismo pkg: **No Convencional** (`b5b58cdc-...`, act. 2026-08-22), **catálogo maestro Cap. IV – Pozos** (`cb5c0f04-...`, con geometría, + SHP `3fcda0c5-...`), **agregado por yacimiento y formación** (`2f2834f4-...`), **por yacimiento y antigüedad** (`adf793e7-...`), **padrón con fecha de primera producción** (`5578dd48-...`), **listado de pozos por operadora** (`cbfa4d79-...`), series históricas por cuenca (`a3244ddd-...`, `af8c50bb-...`) | mismo pkg | CSV (+SHP) | **[?]** | El padrón de primera producción es clave para DCA; los agregados por yacimiento evitan reconstruir el join | Mensual/estático | 2006–2026 | Vía `empresa`/operadora | Existencia **[V]**, esquemas **[?]** |
| **Fractura de pozos (Adjunto IV)** | pkg `71fa2e84-0316-4a1b-af68-7f35e41f58d7`, recurso `2280ad92-6ed3-403e-a095-50139863ab0d` | CSV (+1 PDF) | **[?]** | longitud de rama horizontal (m), etapas de fractura, tipo de terminación, arena nacional/importada (t), agua inyectada (m³), CO₂ (m³), presión máxima (psi), potencia (hp), fechas inicio/fin, empresa | **Diaria** (act. 2026-09-04) | — | Por empresa informante | **Sí [V]**; *"Datos preliminares sujetos a revisión"* |
| **Perforación de pozos** (21 recursos) | pkg `7ea2ac77-d7a0-4129-9fbf-6f1a25d94e21` | CSV/ZIP | **[?]** | Metros perforados por empresa (`3b6b2a2d-...`), pozos terminados por concepto (`42c4eafa-...`) y por tipo (`284e9bee-...`), pozos en perforación (`7fcd6c41-...`) | Mensual | desde 2009 (+legacy) | Desagregación por empresa **[V]** | Existencia **[V]** |
| **Precios en Surtidor (Res. 314/2016)** | pkg `1c181390-5045-475e-94dc-410429be4b17` | CSV (3 recursos) | **[?]** | Vigentes `80ac25de-...` (datastore **True**); **Históricos** `f8dda0d5-...` (datastore **True**) | Continua | — | Por bandera/empresa | **Sí [V]** (datastore verificado campo a campo) |
| **Precios y volúmenes EESS (Res. 1104/04)** — 29 recursos | pkg `708f9ab4-829b-4f02-b507-f303c5bc4800` | CSV + SHP | **[?]** | Precio final con impuestos, volúmenes, EESS y operadores | Mensual/anual | 2004–2024+ | Por bandera | **Sí [V]**: **8 de 29 recursos SIN datastore** (2005-2009, 2012, 2013, 2015) |
| **Reservas de petróleo y gas** — 40 recursos | pkg `reservas-de-petroleo-y-gas`; ZIP 2024 en `http://www.energia.gob.ar/contenidos/archivos/Reorganizacion/informacion_del_mercado/mercado_hidrocarburos/informacion_estadistica/reservas/reservas_al_31-12-2024.zip` | **ZIP → 1 XLSX**, 2 hojas (`fin de concesión`, `fin de vida útil`) | 314.161 B comprimido / 415.774 B | Fila 7: `OPERADOR, CUENCA, PROVINCIA, CONCESIÓN O PERMISO, YACIMIENTO`; matriz Conv./No Conv. × Reservas/Recursos × Comprobadas/Probables/Posibles × PET(Mm³)/GAS(MMm³) | Anual | 2004–2024 | `OPERADOR = 'YPF S.A.'` **[V]** | **Sí [V]** (ZIP descargado y abierto). Encabezado de 7 filas, **solo filas 1, 3, 4 y 5 fusionadas** **[X]** |
| Trayectorias de Pozo Vaca Muerta (`trayectoria-de-pozos`) y Yacimientos (pkg `7378520e-...`, ubicación y polígonos) | portal CKAN | CSV + SHP | **[?]** | Join hacia producción por `areayacimiento`/`idareayacimiento` | Estático | — | — | Existencia **[V]**, esquema **[?]** |
| **3W Petrobras (telemetría de pozo)** | `https://github.com/petrobras/3W` | **Parquet** (pyarrow + brotli) | 1,74 GB declarado / ~1,87 GB medido; **subset clases 0+2+7 ≈ 318 MB**; solo clase 2 = 18,5 MB | 27 variables: `timestamp, P-PDG, P-TPT, T-TPT, P-MON-CKP, T-JUS-CKP, P-JUS-CKGL, QGL, ABER-CKP, ESTADO-*`, + `class`, `state` | **No verificada en texto oficial** (citada en literatura como ~1 Hz) **[?]** | — | N/A (Brasil) | **Sí [V]**, archivo por archivo vía API GitHub. **CC BY 4.0** para datos, Apache 2.0 para código |
| Equinor Volve | `https://www.equinor.com/energy/volve-data-sharing` → Databricks Marketplace | Delta Sharing / Unity Catalog (no archivo plano) | ~40.000 archivos; **GB/TB no confirmados [?]** | **[?]** | **[?]** | — | N/A | **Ambiguo**: acceso verificado **[V]**, texto completo de la "Equinor Open Data Licence" **NO leído [?]** |
| NASA C-MAPSS | PCoE NASA vs. `data.nasa.gov/dataset/c-mapss-...` | ZIP | **[?]** | 30 parámetros de motor/vuelo | **1 Hz [V]** | — | N/A | **Contradictorio**: PCoE dice disponible; data.nasa.gov dice *"CURRENTLY UNAVAILABLE FOR DOWNLOAD"*, licencia *"not specified"* **[?]** |
| Sodir/NPD FactPages (wellbores) | `https://factpages.sodir.no/en/wellbore/tableview/exploration/currentyear` | **EXCEL, XML, CSV [V]** | 23 pozos en la vista current-year | Wellbore name/NPDID, fechas, operador, licencia, purpose, content, status, edad geológica | Continua | Año en curso | N/A | **Sí [V]** |
| SEC EDGAR (20-F de YPF) | `https://data.sec.gov/submissions/CIK0000904851.json`; 20-F FY2025 `https://www.sec.gov/Archives/edgar/data/904851/000119312526126363/d95578d20f.htm` | JSON / HTML | HTML 10,9 MB | Submissions, companyfacts (XBRL), companyconcept, frames | Por presentación | 2019–2026 | CIK 0000904851 | **Sí [V]** — *"These APIs do not require any authentication or API keys to access"* |
| EIA Open Data (Brent/WTI) | `https://www.eia.gov/opendata/` | JSON/CSV/bulk | — | Series de precios spot | Diaria; bulk 2×/día | — | N/A | API key gratuita **[V]**; **rate limits no documentados [?]**; rutas `petroleum/pri/spt` **no reconfirmadas [?]** |
| BCRA Estadísticas Cambiarias | `https://estadisticas-cambiarias.bcra.apidocs.ar/` (base `https://api.bcra.gob.ar`) | JSON | — | `/Maestros/Divisas`, `/Cotizaciones`, `/Cotizaciones/{codMoneda}` | Diaria | — | N/A | Endpoints **[V]**; parámetros de fecha, auth y rate limits **no documentados [?]** |
| Open-Meteo Historical (ERA5 / ERA5-Land) | `https://open-meteo.com/en/docs/historical-weather-api` | JSON/CSV/XLSX | — | temp, humedad, precipitación, viento, nubosidad, suelo, radiación | Horaria y diaria | ERA5 desde 1940; ERA5-Land desde 1950 | N/A | **Sí [V]**: ERA5 **0,25° (~25 km)**, ERA5-Land **0,1° (~11 km)** |
| CAMMESA (vía datos.energia.gob.ar) | `http://datos.energia.gob.ar/dataset/publicaciones-cammesa` | **[?]** | **[?]** | Generación por máquina (MWh), balance MEM, consumo de combustible **[?]** | **[?]** | **[?]** | N/A | **Portal accesible [X]** (200 OK en http; un solo 301 desde https; `Strict-Transport-Security: max-age=0`). Contenido del dataset **no leído [?]** |
| Generador sintético de perforación | `https://github.com/SyntheticFunk/drilling-telemetry-simulator` | Python (salida configurable) | N/A | Telemetría EDR + trayectoria direccional | Configurable | — | N/A | Existencia y **Apache 2.0 [V]**; calidad técnica **[?]** (2 stars, mantenimiento dudoso) |

**Nota transversal de ingesta [V]:** todo el portal fuerza HTTP plano — `https://datos.energia.gob.ar/...` responde **301 a `http://`** (un solo salto, no un bucle **[X]**) y envía `Strict-Transport-Security: max-age=0`. Cualquier cliente que reescriba automáticamente a HTTPS fallará. El pipeline debe usar `curl -L` o `requests` con `allow_redirects=True` sin forzar upgrade.

### 4.2 Combinaciones interesantes (todas construibles con lo verificado)

1. **Producción por pozo × Fractura × Padrón de primera producción** → *feature store* de no convencional: `prod_pet/prod_gas` como target; rama horizontal, etapas, arena y agua inyectada como features de completación; y la fecha de primera producción para normalizar el eje a "meses desde primera producción" (base de toda *type curve* y de DCA).
2. **Producción por pozo × Reservas (XLSX)** → tasa de reposición de reservas por operador y cuenca (reservas comprobadas al cierre vs. producción del año, filtrando `OPERADOR = 'YPF S.A.'`). Obliga a resolver el despivoteo del encabezado jerárquico: ejercicio de ingeniería vistoso y real.
3. **Precios en Surtidor (histórico) × Volúmenes EESS × BCRA FX × EIA Brent/WTI** → márgenes y elasticidad de demanda por bandera y provincia; réplica honesta del caso de micropricing del RTIC de Comercialización.
4. **Producción por pozo × Yacimientos (polígonos) × Open-Meteo ERA5-Land** → efecto de temperatura y viento sobre producción e inyección; a ~11 km la grilla diferencia yacimientos.
5. **3W Petrobras (streaming) × catálogo de pozos de Vaca Muerta** → telemetría real replayed hacia Kafka, mapeada a `idpozo` argentinos (declarando el mapeo como sintético).
6. **20-F (XBRL companyfacts) × producción física por pozo** → reconciliación entre lo declarado al regulador argentino y lo reportado a la SEC: *data quality* entre dominios, que casi ningún portfolio muestra.
7. **Perforación por empresa × Fractura diaria** → indicador tipo "Toyota Well": metros perforados y etapas por equipo/mes contra la meta pública de reducir el ciclo de construcción de pozos un **15–30%** (cita de Micaela Julieta Cecchini) **[V]**.

---

## 5. Qué habría que simular, con qué fidelidad y cómo calibrarlo

El dataset núcleo es **mensual por pozo**: no alcanza para el módulo que más impresiona (RTIC-like). La estrategia correcta es **híbrida de tres capas**, con honestidad explícita sobre qué es real y qué no.

### 5.1 Capa A — Telemetría de pozo: dato REAL (no simular)

Usar **3W de Petrobras**, subset clases 0 (NORMAL, 594 instancias reales, 162 MB), 2 (18,5 MB) y 7 (137 MB) ≈ 318 MB, replayed cronológicamente hacia Kafka/Kinesis desde Parquet. **Fidelidad: máxima — porque no se simula nada.** Único parámetro a fijar: la velocidad de replay, ya que la frecuencia de muestreo nativa no está confirmada oficialmente **[?]**; documentar el supuesto (p. ej. 1 muestra/segundo) como parámetro configurable, no como hecho.

### 5.2 Capa B — Telemetría de perforación: simulación calibrada

Las 27 variables de 3W no cubren las **"más de 60 variables"** del RTIC ni el ciclo de perforación. Aquí sí corresponde simular, con `drilling-telemetry-simulator` (Apache 2.0) o un generador propio. Parámetros de calibración, todos anclados en cifras verificadas:

| Parámetro del simulador | Valor a usar | Anclaje |
|---|---|---|
| Equipos concurrentes (particiones Kafka) | **13** típico, 16 como techo de capacidad | Infobae: *"Hoy tenemos 13 equipos perforando"*; 16 = capacidad de diseño **[X]** |
| Variables por equipo | 60–100 crudas + ~80 KPIs derivados | RTIC Puerto Madero: *"más de 60 variables"*, "100+ variables y 80+ KPIs" |
| Volumen por pozo | ~35 M registros | *"35 millones de datos por pozo"* (dos fuentes independientes) |
| Pozos/año | 200–210 | Meta anual declarada del RTIC |
| Geometría del pozo | superficial ≤800 m; intermedio 800–2.300 m; producción hasta 8.300 m; lateral hasta 5.170 m; caso extremo: 342 etapas, rama 4.600 m | RTIC Puerto Madero / PAD 346 Loma Campana |
| Sets de fractura concurrentes | 8 | RTIC Puerto Madero |
| Turnos / disponibilidad | 24/7, turnos 6–18 h y 18–6 h; 7 h en diagrama 7×7 | RTIC |
| Interrupciones de enlace | Modelar cortes de Starlink → *late data* | Starlink confirmado como transporte; Mbps **no verificados [?]** |

**Cómo calibrar contra datos reales:** las distribuciones no se inventan, se ajustan a los agregados públicos. La producción acumulada simulada de un pozo debe converger al `prod_pet`/`prod_gas` mensual real de ese `idpozo`; la longitud de rama, etapas y toneladas de arena se muestrean de la distribución empírica del CSV de Fractura filtrado por YPF y formación; los metros perforados mensuales agregados deben reproducir la serie de "Metros perforados por empresa". Esa es la diferencia entre un simulador de juguete y uno defendible: **un test de reconciliación que falla si la simulación se aleja del agregado real**.

### 5.3 Capa C — SCADA de refinería y ventas por estación

**Refinería (RTOR La Plata).** Prudencia obligatoria: "200.000 variables" y "+20% de rentabilidad" están **refutadas** **[X]**. Lo verificado: >210.000 bbl/día, 70% shale de Vaca Muerta, 180 km de fibra, 16 km de canalizaciones, 4 km de bandejas, 20 t de soportes, 30 tableros de control. Recomendación: **parametrizar el número de tags** (`N_TAGS`, arrancando en unos pocos miles), modelar los 30 tableros como 30 grupos lógicos de tags, y anclar la simulación al único número duro disponible —el balance de masa de 210.000 bbl/día— para que los caudales simulados cierren contra esa capacidad. Aclarar en el README que la cifra de variables de la RTOR no tiene respaldo primario.

**Ventas por estación.** El nivel agregado no se simula: **Precios en Surtidor históricos + Volúmenes EESS son reales**. Lo que sí se simula es la **granularidad transaccional** (ticket por surtidor y franja horaria):
- Escala: **1.600+ estaciones**, **2.400 camiones** **[V]**.
- El total mensual simulado por estación/producto debe cuadrar con el volumen real del dataset EESS (test de reconciliación).
- Los precios provienen del dataset real; solo el reparto intra-mes es sintético.
- **Feature de conteo vehicular**: generar una serie correlacionada con las ventas a un **97%** — la cifra está verificada y es un anclaje de calibración precioso, porque es un objetivo numérico explícito para el generador.
- Efecto de franja horaria: el caso reporta **+35% de rentabilidad nocturna**; usarlo como parámetro del perfil horario, marcándolo como calibración y no como predicción.

**Regla de oro para todo el bloque:** cada tabla del lakehouse lleva una columna o propiedad de metadatos `data_origin ∈ {real, simulated, derived}`, y el README expone una tabla de trazabilidad. Frente a un entrevistador, admitir con precisión qué es sintético vale más que aparentar que todo es real.

---

## 6. Límites verificados de AWS Free Tier 2026 y estimación de costos

### 6.1 El modelo post-15/16-jul-2025

- Cuentas nuevas reciben **USD 100 al registrarse, *"regardless of your account plan"*** + hasta USD 100 adicionales por actividades (lista exacta **no verificada [?]**; solo mención genérica a "servicios como Amazon EC2 y Amazon Bedrock") **[V]**.
- *"Your free account plan ends after six months or when your credits are fully used – whichever occurs first."* **[V]**
- **Lo determinante:** *"After your free account plan expires, your account closes automatically, and you lose access to your resources and data. AWS retains your content for 90 days before permanently deleting your account and all associated resources."* **[V]**
- La diferencia entre planes **no es el crédito**, sino el acceso a ofertas: Free plan = solo Always Free; Paid plan = Always Free + short-term trials **[V, corrige barrido previo]**. Anuncio del **16-jul-2025** (no 15) **[V]**; 200+ servicios, todas las regiones salvo GovCloud (US) y China.
- Exclusiones: texto genérico (*"...services and features that could possibly deplete your credits, or hardware purchases. Some service examples include Savings Plans, Reserved Instances, and certain AWS Marketplace offers"*); **no hay lista nominal [?]**. Confirmado que EC2, S3, Aurora, RDS, DynamoDB, SageMaker AI y Bedrock figuran disponibles y que **Bedrock AgentCore es exclusivo de Paid plan** **[V]**. Glue, Athena, Kinesis, Step Functions, EventBridge, ECS Fargate, EMR Serverless, MWAA, CloudFormation e IAM quedan **NO VERIFICADOS** en el Free account plan **[?]**.

### 6.2 Tabla de servicios

| Servicio | Tipo de free tier | Límite exacto (cita) | Disponible en Free account plan | Verificado |
|---|---|---|---|---|
| **Step Functions** | **Always Free explícito** | 4.000 transiciones/mes; *"does not automatically expire at the end of your 12 month AWS Free Tier term, and is available to both existing and new AWS customers indefinitely"*; $0,000025/transición extra (us-east-1) | **[?]** | **[V]** |
| **DynamoDB** | Always Free | 25 WCU, 25 RCU, 25 GB storage, 2,5 M lecturas de stream, 1 GB de transferencia saliente (15 GB los primeros 12 meses) | Sí, listado | **[V]** |
| **Lambda** | Always Free (histórico) | 1 M requests + 400.000 GB-segundos/mes | **[?]** | **[V]** (cifra) |
| **SQS** | Always Free | *"All customers can make 1 million Amazon SQS requests for free each month"* | **[?]** | **[V]** |
| **CloudWatch** | Always Free ("permanently free tier benefits") | 10 métricas, 1 M API requests, 10 alarmas, 3 dashboards, 5 GB de logs, 1.800 min de Live Tail | **[?]** | **[V]** |
| **EventBridge** | Always Free (parcial) | Scheduler: 14.000.000 invocaciones/mes; Schema Registry discovery: 5 M eventos/mes; eventos de servicios AWS ingeridos gratis | **[?]** | **[V]** |
| **Glue Data Catalog** | Always Free (implícito) | *"The first million objects stored are free, and the first million accesses are free"* | **[?]** | **[V]** |
| **Glue ETL / Crawlers / Interactive Sessions** | **Sin free tier** | $0,44/DPU-hora facturado por segundo; ejemplo oficial: job de 15 min con 6 DPU = $0,66 | **[?]** | **[V]** |
| **Glue Data Quality (DQDU)** | Sin free tier explícito | $0,44/DPU-hora (mín. 2 DPU, 1 min); anomaly detection: 1 DPU por *statistic* | **[?]** | **[V]** |
| **Athena** | **Sin free tier** | $5 por TB escaneado | **[?]** | **[V]** |
| **CloudFormation** | Always Free aparente | 1.000 *handler operations*/mes; primeros 30 s por operación sin cargo; excedente $0,0009/op + $0,00008/s. **Solo aplica a resource types de terceros y hooks personalizados — los recursos `AWS::*` no generan cargo de CloudFormation** | **[?]** | **[V]** |
| **S3** | 12 meses (no confirmado si varía por región) | 5 GB, 20K GET, 2K PUT | Sí, listado | **[P]** — cifra del barrido; la página de pricing no documenta diferencias regionales **[?]** |
| **Redshift Serverless** | **Trial temporal** | **$300 en créditos por 90 días** desde el registro, solo para cuentas que nunca usaron Redshift Serverless | N/A | **[V]** |
| SNS, Kinesis, EC2, RDS, MWAA, EMR Serverless, SageMaker, ECR/ECS Fargate, IAM | **[?]** | No obtenidos en fuente oficial en esta investigación | **[?]** | **[?]** |
| SageMaker Studio Lab | **[?]** | La URL oficial redirige a `studiolab.sagemaker.aws`, que devolvió **HTTP 403** al fetch | N/A | **[?]** — no usar como dependencia crítica |

### 6.3 Estimación de costos

Escenario "portfolio modesto" (10 GB escaneados/mes en Athena, 2 jobs de Glue ETL de 10 min con 2 DPU):

- Athena: 10 GB ÷ 1.024 × $5 ≈ **$0,05/mes**
- Glue ETL: 2 × 0,1667 h × 2 DPU × $0,44 ≈ **$0,29/mes**
- Subtotal Athena + Glue ETL ≈ **$0,35/mes**
- Resto del pipeline (S3, Lambda, Glue Data Catalog, Step Functions, SQS, CloudWatch, EventBridge) dentro de cuotas Always Free ≈ $0
- **Total en Paid account plan con tráfico bajo: USD 1–5/mes** — estimación propia, no cifra oficial **[?]**.

**Riesgos de costo reales** (no el motor de IaC, gratis para recursos `AWS::*`): NAT Gateway, Elastic IPs huérfanas, Athena mal particionado y crawlers de Glue en loop. **Mitigación obligatoria: particionar por `anio`/`mes`/`cuenca` y almacenar en Parquet/Iceberg comprimido; nunca CSV crudo como tabla consultable.**

**Veredicto AWS:** no es viable **$0 exacto más allá de 6 meses** en el Free account plan, porque la cuenta se cierra sola. Plan correcto: usar los 6 meses para construir y grabar la demo, luego pasar a Paid plan dejando vivo solo el subconjunto Always Free (o apagar todo y conservar IaC + video + repo como evidencia).

---

## 7. Límites verificados de Databricks Free Edition y SaaS gratuitos

### 7.1 Databricks Free Edition

| Recurso | Límite exacto (cita textual) | Verificado |
|---|---|---|
| SQL Warehouse | *"One SQL warehouse, limited to a `2X-Small` cluster size"* | **[V]** |
| Jobs | *"Max of 5 concurrent job tasks per account"* | **[V]** |
| Lakeflow Declarative Pipelines | *"One active pipeline per pipeline type"* | **[V]** |
| Notebooks / compute serverless | *"Limited compute size and usage"* (sin cifra) | **[V]** (cualitativo) |
| AI Search (Vector Search) | *"One AI Search endpoint, limited to one search unit"* | **[V]** |
| Databricks Apps | *"Up to 3 Databricks Apps per account"*, ejecución *"up to 24 hours after being started, updated, or redeployed"* | **[V]** |
| Lakebase (Postgres gestionado) | *"One Lakebase project per account"* | **[V]** |
| Workspace / metastore | *"One workspace and one metastore per account"* | **[V]** |
| **Egress de red** | *"Custom compute configurations are not supported. Additionally, outbound internet access is restricted to a limited set of trusted domains."* — **la allowlist NO es pública** | **[V]** la restricción / **[?]** la lista |
| Verificación por LinkedIn | Amplía el acceso saliente; no se aclara si a dominios arbitrarios o a una allowlist mayor | **[P]** |
| Storage por workspace (GB/TB) | **No publicado en ninguna página oficial revisada** | **[?]** |
| Custom workspace storage locations | No soportadas. Si eso bloquea *external locations* de Unity Catalog hacia un S3 propio: **zona gris** | **[V]** / **[?]** |
| Uso comercial | *"Free Edition accounts may not be used for commercial purposes"* / *"meant for non-commercial use"*. **No hay definición de "commercial purposes"** → un portfolio personal es zona gris | **[V]** cita / **[?]** interpretación |
| Inactividad | *"Databricks may delete Free Edition accounts that are inactive for a prolonged period"* — sin plazo numérico | **[V]** / **[?]** cifra |
| Fair use | Al exceder cuotas: *"your workspace's compute resources will be shut down and unavailable for the rest of the day (and in extreme cases, the rest of the month)"* | **[V]** / **[?]** umbral |
| Community Edition | *"Free Edition replaced the legacy Databricks Community Edition, which was retired in 2025"* — **sin migración automática de datos** | **[V]** |
| Asset Bundles / Terraform | Error reproducido: `error downloading Terraform: Get "https://releases.hashicorp.com/terraform/1.5.5/index.json": dial tcp: lookup releases.hashicorp.com ... server misbehaving`. La restricción de egress es oficial **[V]**; que `releases.hashicorp.com` esté fuera de la allowlist es **hipótesis de comunidad** **[?]** | **[P]** |
| MLflow, Model Serving, Auto Loader, Structured Streaming, Lakeflow Connect, Genie/AI-BI | **NO DOCUMENTADO por edición** | **[?]** |

### 7.2 SaaS gratuitos complementarios

| Servicio | Límites verificados | Uso propuesto |
|---|---|---|
| **GitHub Actions** | Free: **2.000 min/mes**, 500 MB de artifacts; cache 10 GB/repo; *"GitHub Actions usage is free ... for public repositories that use standard GitHub-hosted runners"* → **repo público = minutos ilimitados** **[V]** | **Capa de ingesta** (egress libre hacia `http://datos.energia.gob.ar`) + CI/CD + tests + calidad de datos |
| **Neon** (Postgres serverless) | 0,5 GB/proyecto, 100 CU-hours/mes, hasta 100 proyectos, 10 branches/proyecto, autosuspend a 5 min **no desactivable** **[V]** | Capa serving / metadata del pipeline; branches por entorno dev/staging/prod |
| **Supabase** | 500 MB de DB (shared CPU, 500 MB RAM), 1 GB de file storage, **5 GB de egress + 5 GB de egress cacheado (cuotas separadas)**, 500.000 invocaciones de Edge Functions, 50.000 MAU de Auth, **2 proyectos activos**, pausa tras 1 semana de inactividad **[V]** | API + auth si el proyecto expone un backend |
| **Grafana Cloud Free** | 10k series activas/mes (retención 14 días); **50 GB/mes de logs, 50 GB de traces y 50 GB de profiles, cuotas separadas** (retención 14 días); 3 usuarios de Grafana Assistant con 40 M tokens c/u; soporte comunitario **[V]** | Observabilidad del pipeline: métricas de jobs, logs, alertas de SLA |
| **LocalStack Hobby** | Gratuito, 30+ servicios emulados, 1 sandbox personal, tests en CI. **Kinesis Data Streams SÍ está incluido en Hobby** **[X]**; **Glue y Athena requieren plan Ultimate (USD 89/mes anual)**, no bastan con Base (USD 39/mes anual, USD 45 mensual) **[X]** | Emulación local de S3/Lambda/SQS/**Kinesis**; **no** para Glue/Athena |
| **Moto** | Apache-2.0; instalación granular (`pip install 'moto[ec2,s3,all]'`); la lista completa vive en `IMPLEMENTATION_COVERAGE.md` del repo **[V]** | Tests unitarios de la capa S3/DynamoDB/SQS/Lambda sin contenedores |
| Streamlit Community Cloud | **No verificado [?]** | Alternativa a Databricks Apps para el dashboard |

### 7.3 Consecuencias de diseño

- **Un solo SQL warehouse y 5 tareas concurrentes** hacen inviable un diseño de múltiples pipelines paralelos: unificar en **un pipeline secuencial** (bronze → silver → gold) con flujos internos, no un pipeline por dominio.
- **Databricks Apps mueren a las 24 h** de cada despliegue → el dashboard necesita redeploy programado, o conviene usar AI/BI Dashboards / Streamlit / Grafana.
- **La ingesta va fuera de Databricks.** (A) **GitHub Actions**: workflow con `curl`/`requests` que descarga los recursos CKAN y los sube a un **Volume de Unity Catalog** vía CLI/REST API; su cron reemplaza a Auto Loader (no documentado por edición **[?]**). (B) **Docker local**: un contenedor descarga, valida y normaliza con `requests`/`ckanapi` y hace push al Volume — es el patrón real *edge/on-prem ingestion + cloud lakehouse*, mejor de contar como decisión de arquitectura.
- **Prueba barata previa a fijar el diseño (10 min):** crear la cuenta, verificar LinkedIn y ejecutar `requests.get('http://datos.energia.gob.ar/...')` en un notebook. Vale más que cualquier búsqueda documental, porque la allowlist no es pública.
- **No asumir portabilidad de datos** entre ediciones ni persistencia indefinida: todo reproducible desde cero con scripts de setup.

---

## 8. Opción local distribuida con Docker

### 8.1 Por qué es la base, no el plan B

Con Databricks fuera de la ingesta, AWS autodestruyéndose a los 6 meses y LocalStack Hobby sin Glue/Athena, **el entorno local distribuido es el único que se sostiene indefinidamente y sin fecha de vencimiento**. Además, ejecuta el mismo motor (Spark) que se demuestra en la nube.

### 8.2 Plantilla de referencia verificada

`https://github.com/1ambda/lakehouse` — versiones mínimas confirmadas en su README **[V]**:

| Componente | Versión mínima |
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

Usa **perfiles de Docker Compose** combinables (`COMPOSE_PROFILES=trino|spark|flink|airflow docker-compose up`) y atajos `make compose.cdc` / `make compose.stream` **[V]**. **El README no documenta requisitos de RAM/CPU** **[?]**; levantar Spark + Flink + Kafka + Debezium + Trino + Airflow a la vez implica muchas JVM concurrentes y superaría fácilmente 16 GB.

### 8.3 Requisitos y perfiles recomendados

| Perfil | Servicios | RAM estimada | Para qué |
|---|---|---|---|
| **Batch/medallion** (recomendado como base) | MinIO + Spark + Iceberg (o Delta) + Trino + Airflow ligero | ~8–12 GB **[?]** estimado | Producción por pozo, fractura, reservas, precios: todo el núcleo batch |
| **Streaming** (módulo RTIC-like) | + Kafka (1–3 brokers) + productor de replay de 3W | +4–6 GB **[?]** estimado | Detección de anomalías de pozo en near-real-time |
| **CDC** (opcional/stretch) | + Postgres + Debezium | +2–4 GB **[?]** estimado | Simular el extractor "tipo SAP" con CDC desde una base transaccional |

**Regla práctica derivada de la investigación:** no levantar el stack completo. Priorizar **Trino + Spark + Iceberg/MinIO** y activar Kafka/Flink solo cuando se demuestre el módulo de streaming. Los repos alternativos (`vutrinh274/local_lakehouse`, `kiyeonjeon21/data-stack-lab`, `lechihoang/Data-lakehouse`) no fueron releídos y sus requisitos siguen **[?]**.

### 8.4 Emulación de AWS en local

- **Moto** para tests unitarios de S3/DynamoDB/SQS/Lambda (Apache-2.0, sin contenedor).
- **LocalStack Hobby** para S3/Lambda/SQS y **Kinesis** (sí está incluido **[X]**); **no** para Glue ni Athena, que exigen plan Ultimate.
- **Sustituto de Athena en local:** Trino sobre Iceberg/MinIO (o DuckDB para exploración). Es funcionalmente equivalente para el propósito del portfolio y no cuesta nada.

---

## 9. Casos de ML aplicables, con referencias

Todos los casos siguientes tienen dataset verificado y anclaje en un problema real y público de YPF.

**1. Detección de anomalías en pozos productores (clasificación multiclase de eventos).**
Dataset: **3W de Petrobras** — 10 clases (NORMAL, ABRUPT_INCREASE_OF_BSW, SPURIOUS_CLOSURE_OF_DHSV, SEVERE_SLUGGING, FLOW_INSTABILITY, RAPID_PRODUCTIVITY_LOSS, QUICK_RESTRICTION_IN_PCK, SCALING_IN_PCK, HYDRATE_IN_PRODUCTION_LINE, HYDRATE_IN_SERVICE_LINE), 27 variables, ventanas `WINDOW`/`STEP` provistas por el toolkit (p. ej. clase 2: WINDOW=180 s, STEP=15 s) **[V]**. Referencias: *"A realistic and public dataset with rare undesirable real events in oil wells"*, JPSE 181 (2019), DOI 10.1016/j.petrol.2019.106223; y *"3W Dataset 2.0.0..."*, Scientific Data 13, 949 (2026), DOI 10.1038/s41597-026-07225-z **[V]**. Anclaje YPF: el agente **Nova** *"anticipa desvíos analizando grandes volúmenes de datos operativos"* y **Argus** detecta desvíos "antes de que se conviertan en incidentes" **[V]**. Desafío real de modelado: clases fuertemente desbalanceadas y mezcla de instancias reales, simuladas y *hand-drawn* — un caso perfecto para hablar de estratificación y de métricas más allá del accuracy.

**2. Decline Curve Analysis y *type curves* de Vaca Muerta (forecasting de producción).**
Dataset: producción por pozo (mensual, 2006–2026) + padrón de primera producción + fractura. Target: `prod_pet`/`prod_gas` normalizado a "meses desde primera producción". Baseline: Arps/Duong/SEDM; comparación con LSTM/GRU. **Advertencia [?]: no se pudieron verificar benchmarks de RMSE/MAPE de la literatura DCA con deep learning (paywall OnePetro/SPE) ni type curves publicadas por YPF** — no citar cifras de precisión ajenas; reportar solo las propias. Anclaje YPF: modelo predictivo entrenado con *"más de mil pozos perforados"* **[V]**.

**3. Predicción de productividad por diseño de completación.**
Features de fractura (longitud de rama horizontal, número de etapas, arena nacional/importada, agua y CO₂ inyectados, presión máxima, potencia de equipos) → producción a 12 meses. Es el equivalente analítico de **Toyota Well** (TPS, 3 ejes, 6 frentes, ~100 personas, meta de reducir **15–30%** el ciclo de construcción de pozos) **[V]**. Un modelo interpretable (gradient boosting + SHAP) es más vendible aquí que una red profunda.

**4. Forecasting de demanda y micropricing de combustibles.**
Datasets: Precios en Surtidor (histórico), Volúmenes EESS, BCRA FX, EIA Brent/WTI, Open-Meteo. Anclaje YPF: micropricing con macro, inflación, FX, demanda, franja horaria y precios de competidores; **97%** de correlación conteo vehicular–ventas; **+35%** de rentabilidad nocturna **[V]**. Es el caso con mejor relación entre datos reales disponibles y narrativa de negocio.

**5. Mantenimiento predictivo / RUL como módulo análogo.**
NASA C-MAPSS (30 parámetros, **1 Hz**, vuelos de ~90 min, fallas inyectadas) **[V]**, pero con **disponibilidad y licencia contradictorias entre dos páginas oficiales de NASA** **[?]** — usar solo como referencia bibliográfica, no como dependencia.

**6. RAG / asistente conversacional sobre documentación operativa.**
Anclaje: **GAIA** sobre Azure OpenAI (prototipo funcional en tres meses) y el asistente interno tipo ChatGPT del RTIC que usa el historial de pozos **[V]**. Corpus reproducible: notas de datasets CKAN, el 20-F de YPF (10,9 MB de texto vía EDGAR), boletines de reservas. Databricks Free Edition ofrece **1 endpoint de AI Search con 1 search unit** **[V]** — suficiente para una demo, no para escalar.

**7. Modelo de calidad de datos como caso de ML ligero.**
Detección de outliers en declaraciones juradas de producción (el dataset de fractura advierte *"Datos preliminares sujetos a revisión"* **[V]**), y detección de duplicados entre los recursos de 2024 y 2025 que aparecen con **dos ids distintos** en CKAN **[V]**. Es un caso poco explotado en portfolios y muy valorado en una operadora.

---

## 10. Arquitecturas de referencia y prácticas de producción que conviene mostrar

### 10.1 Estándares de dominio: usar el vocabulario, no desplegar el producto

**OSDU Forum** (The Open Group): **190 organizaciones miembro**, 16 grandes operadoras desarrollando sobre OSDU, 4 proveedores de nube (AWS, Google Cloud, IBM, Microsoft), 170 proveedores/instituciones académicas; existe **implementación de referencia open source** en `https://community.opengroup.org/osdu` (GitLab), desplegable en Azure, AWS, GCP e IBM Red Hat OpenShift; releases recientes "OSDU R3 Milestone 26" y "OSDU Data Platform Standard, Version 1.0" **[V]**. **La madurez de instalación local de esa implementación no fue auditada [?]**.

**PPDM 3.9:** más de 60 áreas temáticas, guías de referencia para 20+ (pozos, sísmica, reservas, producción, estratigrafía, contratos, derechos de tierra), valor estimado superior a **USD 100 millones** en tiempo profesional aportado **[V]**. La cifra de "24 años de desarrollo" **no está confirmada [?]**.

**Energistics / WITSML:** cubre *"Drilling, Completions and Interventions"* (transferencia sitio-a-oficina, eventos, flujos, registros wireline y LWD, trayectorias); **ETP** es *"el método recomendado para asegurar transferencias continuas de datos en tiempo casi real"*; WITS (años 80) era binario, WITSML es XML sobre web **[V]**. PRODML y RESQML **no verificados en detalle [?]**.

**Implementaciones gestionadas** (ninguna con tier gratuito relevante **[V]**): **AWS Energy Data Insights** — AWS Managed Service, ingesta *"de semanas a horas"*, 7 regiones, pago por uso; la persistencia sobre S3/DynamoDB **NO está confirmada [X]**. **Azure Data Manager for Energy** — PaaS en colaboración con SLB, Microsoft Entra ID, múltiples particiones por instancia, integración con SharePoint/Synapse/Power BI/Petrel **[V]**.

**Recomendación:** no desplegar OSDU. **Tomar prestado el vocabulario** (`Well`, `Wellbore`, `WellLog`, `Trajectory`) para nombrar las tablas silver/gold y documentar el mapeo `idpozo` → entidad tipo OSDU; y declarar que el stream de perforación es "al estilo WITSML" en JSON/Avro/Parquet sobre Kafka, justificando por qué no se implementa XML/ETP real. Esa decisión, bien argumentada, demuestra más criterio que implementar el estándar completo.

### 10.2 Prácticas de producción que el proyecto debe exhibir

1. **Arquitectura medallion con contratos de datos** entre capas (esquemas versionados, tipos, nulabilidad, expectativas).
2. **Ingesta de doble ruta**, exigida por la fuente real **[V]**: (a) recursos con `datastore_active=true` → API paginada con posible pushdown SQL; (b) recursos legacy (EESS 2005-2009/2012/2013/2015, Reservas XLSX, tablas dinámicas de perforación) → descarga + parser.
3. **Transformador para semi-estructurados:** despivotar el encabezado jerárquico de 7 filas del XLSX de Reservas (dos hojas, solo filas 1/3/4/5 fusionadas **[X]**) hacia formato largo. Es la pieza más vistosa del bloque batch.
4. **Idempotencia y particionado por año:** verificado que cada CSV anual contiene **solo** ese año **[V]** → el backfill 2006–2026 se paraleliza sin duplicar. Manejar aparte los recursos duplicados de 2024/2025 con dos ids.
5. **Tests de calidad** (Great Expectations / dbt / Glue Data Quality): unicidad `idpozo`+`anio`+`mes`, rangos de `prod_pet`, integridad referencial contra el catálogo maestro, reconciliación de agregados simulados contra reales.
6. **IaC** en Terraform o CloudFormation: los recursos `AWS::*` **no generan cargo de CloudFormation** **[V]**.
7. **CI/CD en GitHub Actions con repo público** (minutos ilimitados **[V]**): lint, tests con Moto, integración con Docker Compose y despliegue.
8. **Observabilidad** en Grafana Cloud Free: métricas de jobs, logs y alertas de frescura/SLA.
9. **Documentación honesta**: tabla de trazabilidad `real / simulado / derivado`, sección "qué verifiqué y qué no", y las correcciones de la sección 13 publicadas en el repo.
10. **Reproducibilidad desde cero**, sin estado manual: Databricks borra cuentas inactivas y AWS cierra la cuenta a los 6 meses **[V]**.

---

## 11. Matriz de decisión: nube vs. local vs. híbrido

| Criterio | AWS Free Tier | Databricks Free Edition | Docker local distribuido | **Híbrido (recomendado)** |
|---|---|---|---|---|
| Costo sostenido | **$0 solo 6 meses**; luego la cuenta **se cierra sola** **[V]**; en Paid plan ≈ **$1–5/mes** **[?]** | $0, pero *"may not be used for commercial purposes"* (portfolio = zona gris **[?]**) y borrado por inactividad **[V]** | $0 indefinido (solo electricidad/hardware) | $0–5/mes, con la parte permanente en local |
| Ingesta desde `http://datos.energia.gob.ar` | Sí (egress libre) | **Alto riesgo de bloqueo**: egress restringido a allowlist no pública + el origen fuerza HTTP plano **[V]/[?]** | Sí, sin restricciones | Sí — ingesta en GitHub Actions o Docker |
| Big data distribuido real | Glue/EMR (pagos) | Spark serverless, pero **1 SQL warehouse 2X-Small y 5 tareas concurrentes** **[V]** | Spark/Trino multi-contenedor, límite = RAM local | Spark local + Spark serverless en Databricks |
| Streaming | Kinesis **[?]** en Free plan | Structured Streaming **no documentado por edición** **[?]** | Kafka real en Compose **[V]** | Kafka local (+ Kinesis como demo acotada) |
| Gobernanza / catálogo | Glue Data Catalog (1 M objetos gratis) **[V]** | **Unity Catalog completo** — la mejor pieza de Free Edition | Iceberg/Hive metastore, sin linaje enterprise | Unity Catalog para el escaparate; Glue Catalog para la demo AWS |
| BI / demo pública | QuickSight **[?]** | AI/BI Dashboards; **Apps mueren a las 24 h** **[V]** | Metabase/Superset local (no público) | Databricks AI/BI o Streamlit + capturas/video |
| IaC | Terraform/CloudFormation, sin costo del motor **[V]** | **Asset Bundles con riesgo de fallo** por egress **[P]**; mitigación: verificación LinkedIn | Compose + Makefile | Terraform (AWS) + Databricks CLI (no Asset Bundles como camino único) |
| Emulación local de servicios | Moto **[V]**; LocalStack Hobby cubre S3/Lambda/SQS/**Kinesis**, **no** Glue/Athena **[X]** | N/A | Nativa | Moto en CI + Trino como sustituto de Athena |
| Riesgo de "portfolio muerto" en el mes 7 | **Alto** (cierre automático) | Medio (inactividad) | **Nulo** | **Bajo** |
| Impacto en un reclutador | Alto (cloud-nativo) | Alto (lakehouse moderno) | Medio-alto (demuestra fundamentos) | **Máximo** (demuestra criterio, no solo herramientas) |

**Decisión recomendada — híbrido en tres anillos:**
- **Anillo 1 (permanente, local):** Docker Compose con MinIO + Spark + Iceberg/Delta + Trino + Kafka (perfil streaming on-demand). Es la fuente de verdad reproducible y no vence nunca.
- **Anillo 2 (escaparate, Databricks Free Edition):** ingesta vía GitHub Actions → Volume de Unity Catalog → transformación silver/gold → SQL Warehouse + AI/BI Dashboard. Es lo que se comparte por link.
- **Anillo 3 (demo cloud, AWS, ventana de 6 meses):** S3 particionado + Lambda + Step Functions + Glue Data Catalog + Athena, todo desplegado con Terraform, grabado en video y documentado. Se apaga al terminar la ventana; el IaC queda como evidencia de que se sabe hacerlo.

Esta topología convierte cada limitación verificada en una decisión de arquitectura explicable — que es precisamente lo que distingue a un ingeniero senior de alguien que siguió un tutorial.

---

## 12. Ideas de proyecto candidatas

### Idea 1 — "RTIC-AR": plataforma lakehouse de producción no convencional con detección de anomalías en streaming
- **Problema de negocio:** replicar la función núcleo del RTIC — detectar desvíos operativos en pozos antes de que se conviertan en pérdida de producción — sobre el universo real de pozos argentinos de YPF.
- **Datasets:** producción por pozo (38 col., 2006–2026, filtro `empresa='YPF S.A.'`, 370.449 filas solo en 2006) + fractura (Adjunto IV, diario) + padrón de primera producción + **3W de Petrobras** (CC BY 4.0) replayed hacia Kafka + catálogo maestro de pozos.
- **Stack:** Docker (Kafka + Spark Structured Streaming + Iceberg/MinIO + Trino) → Databricks Free Edition (Unity Catalog, gold, AI/BI) → Terraform sobre AWS para la demo cloud.
- **Por qué impresiona a YPF:** es el caso emblemático de la compañía, dimensionado con sus propias cifras públicas (13 equipos, 60–100 variables, 35 M datos/pozo), con telemetría **real** de pozos petroleros y no un generador de números aleatorios.
- **Riesgos:** frecuencia de muestreo de 3W no verificada **[?]**; la RAM local limita el número de brokers/ejecutores; Structured Streaming en Free Edition no está documentado por edición **[?]**.

### Idea 2 — "Vaca Muerta Decline Lab": forecasting de producción y type curves
- **Problema:** estimar EUR (Estimated Ultimate Recovery) y construir type curves por formación y yacimiento para priorizar inversión en desarrollo.
- **Datasets:** producción por pozo + producción no convencional + fractura + padrón de primera producción + agregados por yacimiento y por antigüedad de pozo + trayectorias de pozo.
- **Stack:** Spark/Delta + MLflow (tracking de experimentos), baseline Arps/Duong vs. LSTM, feature store en la capa gold, dashboard en AI/BI o Streamlit.
- **Por qué impresiona:** es la analítica que efectivamente hace una operadora de shale, y conecta con el modelo predictivo de YPF entrenado con "más de mil pozos".
- **Riesgos:** semántica exacta de `tef` y `vida_util` y unidades (m³) sin resolver **[?]**; no hay benchmarks públicos verificados para comparar precisión **[?]**; el dato mensual limita la resolución del ajuste temprano de la curva.

### Idea 3 — "Surtidor Analytics": pricing, márgenes y demanda en downstream
- **Problema:** micropricing y forecasting de demanda por estación, producto y franja horaria, con márgenes calculados contra el crudo de referencia y el tipo de cambio.
- **Datasets:** Precios en Surtidor (vigentes + históricos, ambos con datastore activo) + Precios y Volúmenes EESS (con ruta legacy para 8 de 29 recursos) + BCRA `/Cotizaciones` + EIA Brent/WTI + Open-Meteo + transacciones sintéticas calibradas (1.600 estaciones, correlación objetivo 97% con conteo vehicular).
- **Stack:** ingesta dual (API datastore + parser CSV legacy) en GitHub Actions → Delta/Iceberg → dbt para el modelado dimensional → Neon/Supabase como capa serving → Grafana o AI/BI.
- **Por qué impresiona:** replica un caso con resultados publicados por la propia compañía (+35% de rentabilidad nocturna, tiempo de carga de 5 a <3 min) y demuestra manejo de datos económicos y de calendario, no solo de sensores.
- **Riesgos:** rate limits de BCRA y EIA no documentados **[?]**; la parte transaccional es sintética y hay que declararlo con claridad.

### Idea 4 — "Reserve Ledger": pipeline de reservas y tasa de reposición
- **Problema:** calcular el Reserve Replacement Ratio de YPF por cuenca, provincia y concesión, con trazabilidad auditable desde el archivo oficial hasta el KPI.
- **Datasets:** Reservas (40 recursos, ZIP → XLSX de 2 hojas con encabezado jerárquico de 7 filas, filas 1/3/4/5 fusionadas, `OPERADOR` filtrable) + producción por pozo agregada + 20-F de YPF vía `data.sec.gov` (XBRL companyfacts, sin autenticación).
- **Stack:** parser Python (openpyxl/polars) de despivoteo → contratos de datos → Delta/Iceberg → dbt con tests de reconciliación → linaje en Unity Catalog.
- **Por qué impresiona:** es exactamente el tipo de dato semi-estructurado y regulado que rompe los pipelines reales; además cruza el reporte al regulador argentino con el reporte a la SEC — un ejercicio de gobernanza que casi nadie muestra.
- **Riesgos:** el layout del XLSX puede variar entre años (solo se verificó 2024) **[?]**; los boletines de consolidación en PDF no tienen datos tabulares.

### Idea 5 — "SAP-to-Lake": extractor transaccional con CDC y modelo dimensional
- **Problema:** ingerir el sistema comercial y de stock de Downstream (SAP S/4HANA, confirmado en el 20-F FY2025) hacia el lakehouse, sin impactar el transaccional, y conciliarlo con precios y volúmenes reales.
- **Datasets:** Postgres local con esquema comercial inspirado en SAP (materiales, clientes, pedidos, movimientos de stock), poblado y **calibrado** contra los volúmenes reales de EESS; + Precios en Surtidor.
- **Stack:** Postgres + Debezium + Kafka (perfil `compose.cdc`) → Spark → Iceberg → dbt → Trino/Databricks; orquestación en Airflow.
- **Por qué impresiona:** SAP es **la única tecnología del stack de YPF confirmada en un documento firmado ante un regulador**. Un extractor SAP→lake es el caso de uso más defendible de todo el portfolio: nadie puede decir "nosotros no usamos eso".
- **Riesgos:** la base transaccional es sintética (declararlo); Debezium + Kafka + Postgres suman RAM; hay que evitar dar a entender que se accedió al SAP real de YPF.

### Idea 6 — "Energy Data Copilot": RAG sobre documentación técnica y regulatoria del sector
- **Problema:** responder preguntas operativas y regulatorias ("¿cuánto produjo el yacimiento X en 2024?", "¿qué dice el 20-F sobre ciberseguridad?") sobre un corpus heterogéneo, al estilo de GAIA y del asistente interno del RTIC.
- **Datasets:** 20-F FY2025 (10,9 MB) y anteriores vía EDGAR; notas y metadatos de los datasets CKAN; documentación de estándares (OSDU/WITSML); tablas gold del propio lakehouse para *text-to-SQL*.
- **Stack:** embeddings + Databricks AI Search (1 endpoint, 1 search unit) o índice vectorial local; LLM open-source; interfaz en Streamlit; evaluación con set de preguntas de referencia.
- **Por qué impresiona:** conecta con dos casos reales y fechados (GAIA sobre Azure OpenAI, prototipo en 3 meses; asistente conversacional del RTIC) y demuestra que la plataforma de datos sirve de base a productos de IA — el discurso de los 46 agentes de Digital Suppl.AI.
- **Riesgos:** es el módulo con mayor riesgo de parecer un demo de juguete si no se evalúa con rigor; el endpoint único de AI Search no escala; sin *guardrails* de citación, un RAG que alucina cifras destruye la credibilidad del resto del proyecto.

**Recomendación de combinación:** Idea 1 (núcleo, streaming + batch) + Idea 4 (pieza de ingeniería semi-estructurada) + Idea 2 (módulo ML) como proyecto principal, con Idea 5 como extensión si sobra tiempo y capacidad de máquina. Ideas 3 y 6 quedan como módulos vistosos y de bajo riesgo técnico para ampliar el alcance.

---

## 13. Afirmaciones refutadas y correcciones

| # | Afirmación previa | Veredicto | Corrección verificada |
|---|---|---|---|
| 1 | La RTOR del Complejo Industrial La Plata monitorea **200.000 variables** y logró **+20% de rentabilidad** | **REFUTADA** | La fuente no respalda ninguna de las dos cifras. Sí confirma: inauguración **23-dic-2025** (no marzo), capacidad **>210.000 bbl/día** (70% shale de Vaca Muerta), 180 km de fibra óptica, 16 km de canalizaciones, 4 km de bandejas, 20 t de soportes, 30 tableros de control. **No usar "200.000 variables" para fijar el esquema del módulo de refinería.** |
| 2 | YPF controla **16 equipos** de perforación simultáneos desde el RTIC | **REFUTADA** | 16 es la **capacidad de diseño**; la operación real al momento de la nota era **13** (*"Hoy tenemos 13 equipos perforando"*), con meta de 200–210 pozos/año. Dimensionar particiones Kafka con **13**; 16 solo como techo. La cifra alternativa "14" no pudo verificarse de forma independiente **[?]**. |
| 3 | El acuerdo YPF–Corva **se firmó** el 1-sep-2026 | **REFUTADA** | El acuerdo involucra a Horacio Marín y Dharmesh Mehta y fue **anunciado/difundido** el 1-sep-2026 según Diario Río Negro (corroborado por varios medios). Ninguna fuente indica que esa sea la fecha de firma. Reformular como "se anunció". |
| 4 | Los ZIP de Reservas contienen un XLSX con encabezado jerárquico de **7 filas fusionadas** | **REFUTADA (parcial)** | El ZIP 2024 sí contiene un único XLSX, pero tiene **dos hojas** (`fin de concesión`, `fin de vida útil`), y del encabezado de 7 filas **solo las filas 1, 3, 4 y 5 están fusionadas**; las filas 6 (PET/GAS) y 7 (nombres/unidades) son celdas simples. Las columnas `OPERADOR, CUENCA, PROVINCIA, CONCESIÓN O PERMISO, YACIMIENTO` están confirmadas en la fila 7 → el filtro por YPF es viable. |
| 5 | Volve otorga permiso **solo** a instituciones académicas, estudiantes e investigadores | **REFUTADA** | La frase histórica existe en la página, pero la guía de usuario oficial vigente (enero 2026) para acceder vía Databricks Marketplace dice explícitamente *"student, researcher, or professional"* y permite registrarse con *"university or company account"*; la página general de data-sharing declara que *"anyone can use the data"*. **No hay prohibición expresa de uso en portfolio profesional.** Persiste ambigüedad residual: el texto completo de la Equinor Open Data Licence no está accesible **[?]**. |
| 6 | El portal `datos.energia.gob.ar/dataset/publicaciones-cammesa` es inaccesible por un **bucle de redirección 301** | **REFUTADA** | El portal **es accesible**: `http://` devuelve **200 OK** con el contenido real; `https://` hace **una sola redirección 301** hacia `http://`, y el servidor envía `Strict-Transport-Security: max-age=0`, forzando deliberadamente HTTP plano. El "bucle" era un artefacto de herramientas que reescriben http→https. **CAMMESA no debe descartarse como fuente.** |
| 7 | LocalStack Hobby **no incluye Kinesis, Glue ni Athena** | **REFUTADA (parcial)** | **Kinesis Data Streams SÍ está incluido en Hobby.** Glue y Athena figuran como **"Included in Plans: Ultimate"** únicamente — no bastan con Base (USD 39/mes anual, USD 45 mensual); Ultimate cuesta USD 89/mes anual. El argumento para no usar LocalStack Hobby debe apoyarse **solo** en la ausencia de Glue y Athena. |
| 8 | El fallo de Asset Bundles en Free Edition se debe a una restricción de egress **según un colaborador de la comunidad, sin confirmación oficial** | **REFUTADA (parcial)** | La restricción general **SÍ está confirmada oficialmente**: *"outbound internet access is restricted to a limited set of trusted domains"*. Lo que sigue sin confirmación oficial es únicamente si `releases.hashicorp.com` está fuera de esa allowlist. Además, la documentación menciona una mitigación no citada antes: **la verificación de cuenta vía LinkedIn amplía el acceso saliente**. Tratar la restricción como oficial, probar la verificación LinkedIn, y tener plan B (CLI/API directa). |
| 9 | El RTIC de YPF monitorea **"más de 2.000 pozos"** según el artículo del RTIC de Puerto Madero | **REFUTADA** | Esa cifra **no existe** en ese artículo. Lo que sí aparece: **20 equipos de torre** operando en Vaca Muerta, **más de 60 variables** en tiempo real, **8 sets de fractura simultáneos**, **88 profesionales**, **más de mil pozos perforados** (histórico acumulado, no tiempo real) y **35 millones de registros** vía Starlink. Nota de trazabilidad: la cifra de "2.000+ pozos" proviene de un artículo distinto, sobre el **RTIC de Neuquén**; no mezclar fuentes. |
| 10 | AWS Energy Data Insights persiste sobre **S3 y DynamoDB** | **No confirmada** | El anuncio oficial de mayo 2025 no menciona S3 ni DynamoDB como stack de persistencia. Lo confirmado: OSDU tiene implementación de referencia open source en `community.opengroup.org/osdu`, desplegable en Azure, AWS, GCP e IBM Red Hat OpenShift **[V]**. |

**Correcciones menores adicionales verificadas:** el anuncio del nuevo Free Tier de AWS es del **16-jul-2025** (no 15); los **USD 100** iniciales se otorgan *"regardless of your account plan"*; el artículo de El Cronista sobre salarios es del **31-dic-2024** (no marzo 2025); Toyota Well involucra **~100 personas** según la fuente primaria (no 100–250); la meta de exportaciones aparece como **2030** en La Nación y como **2031** en las fuentes de Globant/PRNewswire — discrepancia no resuelta **[?]**; la resolución de Open-Meteo es **0,25° (ERA5) / 0,1° (ERA5-Land)**, no del orden de 1 km; la cifra de membresía de OSDU es **190** organizaciones (no 185+); el dataset 3W pesa **1,74 GB** declarados vs. **~1,87 GB** medidos.

**Vacíos abiertos que NO bloquean el diseño** (resolubles durante el build): semántica exacta de `tef` y `vida_util` y unidades (m³); columnas del recurso "Producción No Convencional" y del catálogo maestro de pozos; contenido de `trayectoria-de-pozos`; si `datastore_search_sql` acepta pushdown real sobre los CSV anuales grandes; benchmarks de DCA con deep learning; rate limits de EIA y BCRA; cifra de storage de Databricks Free Edition; requisitos de RAM de los repos alternativos de lakehouse local.

---

## 14. Fuentes

**YPF — transformación digital y operaciones**
1. https://www.rionegro.com.ar/energia/como-es-el-megacerebro-de-ypf-que-ya-controla-los-pozos-de-vaca-muerta-3926124/
2. https://www.infobae.com/def/2025/08/23/como-son-los-centros-inteligentes-que-monitorean-en-tiempo-real-vaca-muerta-y-las-estaciones-de-servicio-de-ypf/
3. https://mase.lmneuquen.com/vaca-muerta/ypf-monitorea-mas-2000-pozos-vaca-muerta-su-rtic-ia-drones-y-starlink-n1248621
4. https://mase.lmneuquen.com/vaca-muerta/como-es-el-real-time-intelligence-center-el-cerebro-digital-ypf-que-construye-los-pozos-vaca-muerta-n1161568
5. https://www.rionegro.com.ar/energia/ypf-y-corva-renuevan-acuerdo-para-impulsar-la-transformacion-digital-en-vaca-muerta-4705162/
6. https://www.adnsur.com.ar/pulso-energetico/acuerdo-entre-ypf-y-microsoft-para-impulsar-la-gestion-de-contratos-con-inteligencia-artificial_a66cf407c5ae53ef0a7cedc89
7. https://www.rionegro.com.ar/energia/ypf-y-globant-lanzan-un-proyecto-para-acelerar-la-transformacion-digital-con-inteligencia-artificial-4353734/
8. https://www.rionegro.com.ar/energia/ypf-inauguro-la-nueva-sala-real-time-operations-room-en-el-complejo-industrial-la-plata-4412625/
9. https://www.lanacion.com.ar/economia/toyota-well-el-plan-con-el-que-ypf-planea-reducir-el-tiempo-de-construccion-de-pozos-en-vaca-muerta-nid06122024/
10. https://www.prnewswire.com/news-releases/ypf-and-globant-advance-a-major-project-to-transform-and-optimize-the-supply-chain-with-ai-solutions-302598411.html

**YPF — empleo y perfiles**
11. https://direcciona.hiringroom.com/jobs/get_vacancy/69eb716188bebe851ee4fd0c
12. https://www.cronista.com/informacion-gral/ypf-busca-empleados-con-sueldos-de-hasta-un-millon-de-pesos-cual-es-el-puesto-y-cuales-son-sus-requisitos/
13. https://www.bumeran.com.ar/perfiles/empresa_ypf_281260.html (contenido no accesible)
14. https://oportunidades.ypf.com/content/Como-sumar-tu-energia/?locale=es_ES
15. https://ar.linkedin.com/company/ypf-s-a-/jobs
16. https://www.veintitrés.com.ar/actualidad/YPF-ofrece-trabajo-en-Argentina-lista-de-empleos-y-requisitos-20230303-0006.html
17. https://www.iprofesional.com/notas/290587-petroleo-energia-YPF-en-Vaca-Muerta-el-rol-clave-de-la-transformacion-digital

**YPF — documentación regulatoria y corporativa**
18. https://www.sec.gov/Archives/edgar/data/904851/000119312526126363/d95578d20f.htm (Form 20-F FY2025)
19. https://www.sec.gov/Archives/edgar/data/904851/000119312526126363/0001193125-26-126363-index.htm
20. https://www.sec.gov/Archives/edgar/data/904851/000119312525067155/0001193125-25-067155-index.htm
21. https://data.sec.gov/submissions/CIK0000904851.json
22. https://www.sec.gov/search-filings/edgar-application-programming-interfaces
23. https://investors.ypf.com/financial-information.html
24. https://www.microsoft.com/en-us/customers/search?sq=YPF (0 resultados)
25. https://www.databricks.com/customers
26. https://aws.amazon.com/solutions/case-studies/

**Datasets de energía (Argentina)**
27. http://datos.energia.gob.ar/api/3/action/package_show?id=produccion-de-petroleo-y-gas-por-pozo
28. http://datos.energia.gob.ar/api/3/action/package_show?id=datos-de-fractura-de-pozos-adjunto-iv
29. http://datos.energia.gob.ar/api/3/action/package_show?id=perforacion-de-pozos-de-petroleo-y-gas
30. http://datos.energia.gob.ar/api/3/action/package_show?id=precios-en-surtidor
31. http://datos.energia.gob.ar/api/3/action/package_show?id=precios-eess---resolucion-1104-04
32. http://datos.energia.gob.ar/api/3/action/package_show?id=reservas-de-petroleo-y-gas
33. http://datos.energia.gob.ar/api/3/action/package_show?id=produccion-hidrocarburos-yacimientos
34. http://datos.energia.gob.ar/api/3/action/package_show?id=precio-de-exportacion-de-petroleo-crudo
35. http://datos.energia.gob.ar/api/3/action/package_show?id=precio-internacional-pi-del-petroleo-crudo
36. http://datos.energia.gob.ar/api/3/action/package_search?q=produccion&rows=50
37. http://datos.energia.gob.ar/api/3/action/package_search?q=precio%20exportacion
38. http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/4e1c55e5-1f1b-4fc8-aa37-2080d9795f29/download/produccin-de-pozos-de-gas-y-petrleo-2006.csv
39. http://www.energia.gob.ar/contenidos/archivos/Reorganizacion/informacion_del_mercado/mercado_hidrocarburos/informacion_estadistica/reservas/reservas_al_31-12-2024.zip
40. http://datos.energia.gob.ar/dataset/publicaciones-cammesa
41. https://datos.gob.ar/api/3/action/package_search?q=YPF&rows=20
42. https://preciosensurtidor.energia.gob.ar/

**Datasets complementarios y de ML**
43. https://github.com/petrobras/3W
44. https://doi.org/10.1016/j.petrol.2019.106223
45. https://doi.org/10.1038/s41597-026-07225-z
46. https://www.equinor.com/energy/volve-data-sharing
47. https://equinoropendata.blob.core.windows.net/userguides/Equinor%20open%20data%20-%20User%20Guide.pdf
48. https://www.equinor.com/news/archive/14jun2018-disclosing-volve-data
49. https://factpages.sodir.no/en/wellbore/tableview/exploration/currentyear
50. https://www.nasa.gov/intelligent-systems-division/ (PCoE data set repository)
51. https://data.nasa.gov/dataset/c-mapss-aircraft-engine-simulator-data
52. https://github.com/SyntheticFunk/drilling-telemetry-simulator
53. https://www.eia.gov/opendata/
54. https://estadisticas-cambiarias.bcra.apidocs.ar/ (base https://api.bcra.gob.ar)
55. https://open-meteo.com/en/docs/historical-weather-api
56. https://arxiv.org/pdf/2504.13976 (PDF no legible por el extractor; contenido no citable)

**AWS Free Tier**
57. https://docs.aws.amazon.com/en_us/awsaccountbilling/latest/aboutv2/free-tier.html
58. https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html
59. https://aws.amazon.com/free/free-tier-faqs/
60. https://aws.amazon.com/free/
61. https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan/
62. https://aws.amazon.com/glue/pricing/
63. https://aws.amazon.com/athena/pricing/
64. https://aws.amazon.com/step-functions/pricing/
65. https://aws.amazon.com/cloudformation/pricing/
66. https://aws.amazon.com/redshift/free-trial/
67. https://aws.amazon.com/s3/pricing/
68. https://aws.amazon.com/lambda/pricing/
69. https://aws.amazon.com/dynamodb/pricing/
70. https://aws.amazon.com/cloudwatch/pricing/
71. https://aws.amazon.com/eventbridge/pricing/
72. https://aws.amazon.com/sqs/pricing/
73. https://aws.amazon.com/sns/pricing/
74. https://aws.amazon.com/sagemaker/studio-lab/ (redirige a https://studiolab.sagemaker.aws/, HTTP 403)
75. https://www.localstack.cloud/pricing
76. https://github.com/getmoto/moto

**Databricks y SaaS gratuitos**
77. https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
78. https://docs.databricks.com/aws/en/getting-started/free-edition
79. https://docs.databricks.com/aws/en/getting-started/free-trial-vs-free-edition
80. https://docs.databricks.com/aws/en/resources/limits
81. https://docs.databricks.com/aws/en/getting-started/ce-migration
82. https://docs.databricks.com/aws/en/compute/serverless/limitations
83. https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations
84. https://docs.databricks.com/aws/en/dev-tools/cli/
85. https://community.databricks.com/t5/administration-architecture/asset-bundle-on-free-edition/td-p/127236
86. https://neon.com/docs/introduction/plans
87. https://supabase.com/pricing
88. https://grafana.com/pricing/
89. https://docs.github.com/en/billing/concepts/product-billing/github-actions

**Estándares y arquitecturas de referencia**
90. https://www.opengroup.org/osdu-forum/
91. https://community.opengroup.org/osdu
92. https://learn.microsoft.com/en-us/azure/energy-data-services/overview-microsoft-energy-data-services
93. https://aws.amazon.com/about-aws/whats-new/2025/05/managed-support-energy-data-insights/
94. https://energistics.org/witsml-data-standards
95. https://ppdm.org/ppdm/PPDM/IEDS/PPDM_Data_Model/PPDM/PPDM_3.9_Data_Model.aspx?hkey=c8aed1ca-aa85-409e-8d89-74b42a6d2a18
96. https://github.com/1ambda/lakehouse

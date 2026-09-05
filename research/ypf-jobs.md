# Ofertas laborales y perfiles de datos en YPF — lectura profunda de fuentes primarias

**Fecha de investigación:** 2026-09-04
**Objetivo:** verificar con fuentes primarias qué tecnologías de datos e IA exige o usa YPF S.A., para calibrar el stack de un proyecto portfolio de Data Engineering end-to-end sobre YPF que corra en nubes gratuitas (AWS Free Tier, Databricks Free Edition) o en local distribuido.

## Resumen ejecutivo

Tras releer completas las siete fuentes priorizadas (una no cargó contenido útil), la conclusión central del barrido previo se sostiene y se refuerza: **YPF no publica, en ningún aviso de empleo hallado, un stack tecnológico de datos explícito** (no aparecen citados textualmente Databricks, Snowflake, Kafka, Spark, dbt, MLflow, Palantir, Dataiku ni SAP BW). Lo que sí queda confirmado con lectura directa es:

1. Un único aviso técnico verificable (Programa de Jóvenes en Tecnología, HiringRoom) que pide experiencia en "Tecnología / Datos / IA" de forma genérica, sin nombrar herramientas.
2. Rangos salariales por especialidad tecnológica (Cronista), pero la nota tiene fecha real de **31 de diciembre de 2024**, no marzo de 2025 como se había registrado — corrección importante de fecha.
3. El proyecto YPF-Globant "Digital Suppl.AI" (PRNewswire, 29 oct 2025), que confirma 46 agentes de IA y menciona AWS, OpenAI, NVIDIA y Unity como partners de Globant (no necesariamente el stack contratado por YPF).
4. La alianza YPF-Corva para la plataforma RTIC de datos de perforación en tiempo real, cuya renovación —según el artículo releído— se firmó el **1 de septiembre de 2026** (dato más preciso y más reciente de lo registrado en el barrido anterior).
5. La página de empleos de YPF en Bumeran no pudo verificarse en esta sesión (contenido vacío al momento del fetch), por lo que el finding de 2023 sobre "Data Warehouse, Data Lake, Python, PowerShell, R, SQL" queda **sin poder confirmarse ni descartarse** — se recomienda tratarlo como no verificado y no usarlo como base de diseño.

En síntesis: la evidencia primaria sigue sin permitir construir un ranking de tecnologías por frecuencia con solidez estadística. Para el diseño del proyecto portfolio, esto significa que **el stack no debe justificarse citando "requisitos de YPF"**, sino como una elección razonada basada en (a) el mercado argentino de datos en general, (b) las dos señales de infraestructura confirmadas (AWS como nube, Power BI como herramienta de BI), y (c) el free tier disponible de cada herramienta.

---

## 1. Programa de Jóvenes en Tecnología (HiringRoom/Direcciona)

**URL:** https://direcciona.hiringroom.com/jobs/get_vacancy/69eb716188bebe851ee4fd0c

Es el único aviso de YPF con lenguaje textual sobre datos e IA. Datos confirmados en la relectura completa:

| Campo | Valor textual |
|---|---|
| Título | "Programa de Jóvenes en Tecnología de YPF" |
| Vacantes | 10 en total: 7 en Buenos Aires, 3 en Neuquén |
| Modalidad | Híbrida |
| Reporta a | "vicepresidencia de tecnología de YPF" |
| Requisito de experiencia | "2 años de experiencia comprobable en Tecnología / Datos / IA" |
| Carreras | STEM — Tecnología e Informática, Ingenierías, Ciencias Exactas (incluye Ciencia de Datos) |
| Idioma | Inglés avanzado |
| Movilidad | Disponibilidad para "viajar y/o relocalizarte" |
| Contrato | Full-time |
| Tecnologías nombradas | Ninguna herramienta específica — solo "Inteligencia Artificial (IA)", "gestión de datos", "herramientas de IA", "proyectos de evolución digital" |

**Conclusión:** el aviso confirma que YPF agrupa Datos e IA bajo una sola línea de reclutamiento júnior dentro de la Vicepresidencia de Tecnología, pero no lista ninguna tecnología puntual. No hay novedades respecto al barrido previo; se confirma el dato de vacantes (7+3=10) que antes era "medium confidence" y ahora pasa a **high confidence** por lectura directa.

## 2. YPF y Globant — "Digital Suppl.AI" (PRNewswire, 29 oct 2025)

**URL:** https://www.prnewswire.com/news-releases/ypf-and-globant-advance-a-major-project-to-transform-and-optimize-the-supply-chain-with-ai-solutions-302598411.html

Cifras y datos confirmados:

- **46 agentes de IA** distribuidos en **8 soluciones agénticas**.
- Foco de la Fase 1: gestión de procurement, gestión de inventario, gestión de contratos y proveedores.
- Modelo operativo: "Globant AI Pods" (humanos supervisando agentes de IA) y "Energy AI Studio" (unidad especializada de Globant en energía y telecom).
- Meta estratégica citada: exportaciones de Argentina por **USD 30.000 millones para 2031**, vinculada al "Plan 4x4" y "Vision 2030" de YPF.
- Partners tecnológicos mencionados: **AWS, OpenAI, NVIDIA y Unity** — pero el texto los presenta como partners globales de **Globant**, no como proveedores confirmados del proyecto YPF. Esta distinción es crítica: el comunicado no dice "YPF usa AWS/OpenAI para Digital Suppl.AI", dice que Globant es partner de esas empresas en general.
- Citas textuales de ejecutivos:
  - Martín Migoya (CEO de Globant): la asociación busca "redefine the future of Supply Chain in the energy industry".
  - Horacio Marín (CEO de YPF): el proyecto "es clave para tener esas herramientas en marcha" para la transformación productiva de Argentina.
  - Fernando Montero Bolognini (CEO de Energy & Telecom AI Studio, Globant): YPF tendrá "desarrollos de IA de última generación".

**Conclusión:** se mantiene la lectura previa — es la evidencia más concreta de un proyecto de datos/IA de gran escala en YPF, pero sin especificar motor de datos, nube contratada ni frameworks de ML propios del proyecto (más allá de las alianzas generales de Globant).

## 3. Alianza YPF-Corva (plataforma RTIC)

**URL:** https://www.rionegro.com.ar/energia/ypf-y-corva-renuevan-acuerdo-para-impulsar-la-transformacion-digital-en-vaca-muerta-4705162/

Hallazgo relevante de la relectura: la fecha del acuerdo renovado es **1 de septiembre de 2026** (Horacio Marín, CEO de YPF, y Dharmesh Mehta, Executive Chairman de Corva), lo que la convierte en la fuente **más reciente** de todo el corpus — a solo 3 días de esta investigación. Esto corrige/precisa el finding previo, que solo decía "alianza renovada" sin fecha exacta.

Citas textuales:
- "modelo de operación en tiempo real, basado en datos, analítica avanzada e inteligencia artificial"
- "seguir el desempeño de los pozos, anticipar potenciales eventos de riesgo"
- "consolidar al RTIC como el sistema operativo digital para la construcción de pozos"

Tecnologías/conceptos mencionados: analítica predictiva, alertas inteligentes, automatización, "copilotos digitales", agentes de IA, "advisors" (asesores automatizados). **No hay cifras cuantitativas** (pozos, volumen de datos, throughput) en el artículo.

**Conclusión:** RTIC es la plataforma de datos operativos (upstream/perforación) más consolidada de YPF, con foco en series de tiempo de sensores de pozos, analítica en tiempo real e IA aplicada — un ángulo de dominio (oil & gas time-series/IoT) útil para justificar decisiones de modelado de datos del proyecto portfolio (ej. tablas de eventos de pozos, streaming simulado), aun sin poder nombrar el motor tecnológico real.

## 4. Rangos salariales por especialidad tecnológica (El Cronista)

**URL:** https://www.cronista.com/informacion-gral/ypf-busca-empleados-con-sueldos-de-hasta-un-millon-de-pesos-cual-es-el-puesto-y-cuales-son-sus-requisitos/

**Corrección importante:** la fecha real de publicación del artículo es **31 de diciembre de 2024**, no marzo de 2025 como constaba en el barrido previo. Se corrige esta fecha en el registro.

Datos confirmados en esta lectura:

| Área | Rol | Salario mensual (ARS) |
|---|---|---|
| Estaciones de servicio | Jefe de turno | $1.022.063 |
| Estaciones de servicio | Administrativo | $983.023 |
| Estaciones de servicio | Operador | $970.697 |
| Tecnología/Programación | Junior | ~$2.000.000 |
| Tecnología/Programación | Senior/Semi-Senior | $4.000.000–$5.000.000 |
| RRHH | Director | +$7.000.000 |
| RRHH | Gerente | ~$4.500.000 |
| RRHH | Supervisor | $2.400.000 |

Vacantes puntuales citadas en el artículo (no de datos): Supervisor de Biomediación (Santa Cruz), Consultor de Ciberseguridad (CABA), Analista de Servicios Generales (Buenos Aires).

**Nota de discrepancia:** en esta relectura, el resumen extraído **no reprodujo textualmente** el desglose de especialidades "Ciberseguridad, Cloud, IA, ML, Soporte, Análisis de Negocio y Datos" que constaba en el barrido previo como cita directa. Es posible que ese desglose esté en una parte del artículo no capturada por el resumen automático, o que el finding previo haya generalizado a partir de la sección "Tecnología/Programación". Se recomienda **bajar la confianza** de esa lista de especialidades de "high" a "medium" hasta poder confirmarla con una relectura manual o un fetch adicional, aunque los rangos salariales junior (~$2M) y senior ($4-5M) sí quedan confirmados.

## 5. Bumeran — perfil de empresa YPF (no verificable en esta sesión)

**URL:** https://www.bumeran.com.ar/perfiles/empresa_ypf_281260.html

El fetch de esta sesión devolvió una página sin contenido útil (solo caracteres vacíos, probablemente por renderizado dinámico con JavaScript que WebFetch no ejecuta). **No se pudo confirmar ni descartar** el finding previo (de baja confianza) sobre un aviso 2023 que pedía Data Warehouse, modelado de datos, Data Lake, Python, PowerShell, R y SQL. Este finding debe tratarse como **no verificado** — no debe usarse como justificación de diseño del proyecto portfolio sin una relectura exitosa futura (idealmente con un navegador con JS, ej. Claude in Chrome).

## 6. Fuentes no releídas en esta sesión (por agotamiento de presupuesto de búsqueda)

El portal oportunidades.ypf.com y la página de LinkedIn Jobs de YPF ya habían sido revisados en el barrido anterior (sin roles de datos visibles); esta sesión no encontró presupuesto de búsqueda (WebSearch agotado en 200/200 desde el inicio) para hacer las 5 búsquedas adicionales planeadas (YPF+Snowflake, YPF+Azure/GCP 2025-2026, avisos frescos Zonajobs/Computrabajo/Indeed). **Esta es una limitación de esta sesión, no un hallazgo negativo** — las preguntas abiertas del barrido anterior sobre esas combinaciones siguen abiertas.

---

## Tabla consolidada de evidencia por tecnología

| Tecnología | Evidencia directa YPF | Fuente | Confianza |
|---|---|---|---|
| AWS | Alianza de infraestructura para migración de plataforma tecnológica (TCO); partner de Globant en Digital Suppl.AI | iProfesional (no re-verificado en esta sesión), PRNewswire | Media |
| Power BI | Valorado (no excluyente) en 1 aviso de analista de gestión | Veintitrés 2023 | Media (aviso antiguo, no de datos puro) |
| SAP | Requisito excluyente en el mismo aviso de analista | Veintitrés 2023 | Media |
| IA / agentes de IA | 46 agentes de IA en supply chain (Globant); IA en RTIC de Corva | PRNewswire, Rio Negro | Alta (existencia del proyecto), Media (stack técnico) |
| Databricks, Snowflake, Kafka, Spark, dbt, MLflow, Palantir, Dataiku, SAP BW | Sin evidencia directa en ningún aviso o comunicado de YPF | — | Ninguna evidencia (no confirma ni descarta uso interno) |
| Python, SQL, PowerShell, R, Data Warehouse, Data Lake | Finding de 2023 vía Bumeran, no verificable en esta sesión | Bumeran (fetch fallido) | Muy baja / no verificado |

---

## Implicancias para el diseño del proyecto portfolio YPF Data Platform

Dado que la evidencia de "requisitos técnicos citados por YPF" es escasa y de bajo detalle, la recomendación es **no diseñar el stack como respuesta literal a un aviso de empleo de YPF**, sino como:

1. Un proyecto de **ingeniería de datos de dominio oil & gas** inspirado en el caso de uso real y confirmado más concreto (RTIC de Corva: datos de perforación, fractura e intervención de pozos en tiempo real), que se puede simular con datos públicos o sintéticos de pozos de Vaca Muerta.
2. Un stack que corra en **AWS Free Tier** (coherente con la migración de YPF a AWS) y/o **Databricks Free Edition**, ya que son las dos piezas de infraestructura con algo de respaldo documental (AWS confirmado como nube de YPF; Databricks es la plataforma "genérica" de facto en el mercado de consultoras que reclutan para el sector, aunque no específicamente citada por YPF).
3. Justificar en el propio README del proyecto que el stack se eligió por relevancia de mercado y no por un requisito textual de YPF, dado que ningún aviso de la petrolera nombra herramientas de datos concretas más allá de SAP/Power BI en un aviso administrativo.
4. Considerar Power BI o un dashboard equivalente (ej. Metabase/Superset gratuitos) como capa de BI final, dado que es la única herramienta de visualización con mención textual directa en un aviso de YPF.

---

## Fuentes

1. https://direcciona.hiringroom.com/jobs/get_vacancy/69eb716188bebe851ee4fd0c
2. https://www.prnewswire.com/news-releases/ypf-and-globant-advance-a-major-project-to-transform-and-optimize-the-supply-chain-with-ai-solutions-302598411.html
3. https://www.rionegro.com.ar/energia/ypf-y-corva-renuevan-acuerdo-para-impulsar-la-transformacion-digital-en-vaca-muerta-4705162/
4. https://www.cronista.com/informacion-gral/ypf-busca-empleados-con-sueldos-de-hasta-un-millon-de-pesos-cual-es-el-puesto-y-cuales-son-sus-requisitos/
5. https://www.bumeran.com.ar/perfiles/empresa_ypf_281260.html (contenido no accesible en esta sesión)
6. https://oportunidades.ypf.com/content/Como-sumar-tu-energia/?locale=es_ES (revisado en barrido previo, no re-fetched en esta sesión)
7. https://ar.linkedin.com/company/ypf-s-a-/jobs (revisado en barrido previo, no re-fetched en esta sesión)
8. https://www.veintitrés.com.ar/actualidad/YPF-ofrece-trabajo-en-Argentina-lista-de-empleos-y-requisitos-20230303-0006.html (citado del barrido previo)
9. https://www.iprofesional.com/notas/290587-petroleo-energia-YPF-en-Vaca-Muerta-el-rol-clave-de-la-transformacion-digital (citado del barrido previo, no re-verificado)

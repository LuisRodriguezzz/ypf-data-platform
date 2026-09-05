# Transformación digital y stack de datos de YPF — lectura profunda de fuentes primarias

**Fecha de investigación:** 2026-09-04
**Método:** lectura completa (WebFetch) de 8 fuentes seleccionadas del barrido previo, más intento de 5 búsquedas adicionales (WebSearch agotó su cupo de sesión antes de poder ejecutarlas — ver sección de preguntas abiertas).

## 1. Resumen ejecutivo

YPF sostiene un programa de transformación digital ("Plan 4x4", liderado por el CEO Horacio Marín) cuyo componente más documentado en prensa es una red de **Real Time Intelligence Centers (RTIC)** y salas de operación en tiempo real (RTOR), cada uno con cifras propias de variables, pozos, cámaras y personal. La lectura completa de las 8 fuentes confirma casi todos los findings del barrido previo, pero **corrige de forma importante uno de ellos** (la RTOR de La Plata) y **agrega precisión numérica** a varios más (turnos, personal, profundidades de pozos, infraestructura física). No se encontró en estas 8 fuentes ninguna mención directa a AWS, Google Cloud, Databricks, Snowflake, Palantir, SAP o AVEVA/OSIsoft PI; la única nube confirmada sigue siendo Microsoft Azure, exclusivamente para GAIA/Y-Click!.

Las 5 búsquedas adicionales planificadas para cerrar preguntas abiertas (proveedor de nube de los RTIC, uso de AVEVA PI, contrato SAP, proveedor del gemelo BIM de Punta Colorada, y detalle de Y-TEC) **no pudieron ejecutarse**: el cupo de WebSearch de la sesión se agotó antes de correrlas. Esas preguntas permanecen abiertas y se documentan al final con la búsqueda exacta que se recomienda repetir en una sesión nueva.

## 2. Hallazgos verificados por fuente

### 2.1 RTIC Puerto Madero — Upstream Vaca Muerta (Río Negro, art. "megacerebro")

Cifras confirmadas textualmente:
- **35 millones de datos por pozo** que el sistema es capaz de procesar.
- **80 variables** monitoreadas mediante IA (el barrido previo decía "80-100 variables/indicadores"; esta fuente específica solo confirma 80).
- **130 pantallas** de última generación, en una sala de **350 m²**, piso 26 de la Torre YPF (la fuente la llama "Torre Pelli"), Puerto Madero.
- **14 perforadores (equipos de perforación)** en actividad simultánea — nota: el barrido previo mencionaba "hasta 16"; esta fuente puntual dice 14. Otra fuente (Infobae, ver 2.2) menciona "4 equipos por puesto de trabajo, 16 equipos simultáneos posibles, 13 operativos actualmente" — es decir, 16 es la capacidad máxima de diseño y 13-14 el número real operando, cifras compatibles entre sí pero que conviene reportar con ese matiz (capacidad de diseño vs. operación real).
- Monitoreo 24 horas, turnos de **7 horas** en diagrama **7×7**.
- Caso destacado: **PAD 346** en Loma Campana — 6 pozos, **342 etapas de fractura** (récord), pozo "slim" con rama horizontal de **4.600 metros**; monitoreo iniciado el 28 de octubre en Neuquén y transferido a Puerto Madero dos semanas después.
- Construcción de la sala: **4 meses y medio**. Inauguración: **13 de diciembre de 2024** (Día del Petróleo).

### 2.2 RTIC Puerto Madero — Upstream y Comercialización (Infobae, agosto 2025)

**RTIC Vaca Muerta (ampliación):**
- **88 profesionales** en turnos rotativos, **7 unidades operativas**, operación 24/7, dos turnos (6-18h y 18-6h).
- Detalle de perforación direccional en tres tramos: superficial (hasta 800 m), intermedio (800-2.300 m), producción (profundidad récord de **8.300 m**), y ramas laterales de hasta **5.170 m**.
- **4 equipos de perforación por puesto de trabajo**, hasta 16 equipos simultáneos de capacidad; **13 equipos operativos** actualmente. Meta: **200-210 pozos por año**.
- Confirma "35 millones de datos por pozo" y agrega **más de 100 variables y 80+ KPIs** (concilia la cifra de "80-100" del barrido previo).
- **90 cámaras** en yacimientos, red Starlink para transmisión en tiempo real.

**RTIC Comercialización (downstream):**
- **1.600+ estaciones de servicio**, **2.400 camiones** rastreados por geolocalización, datos en tiempo real de cada surtidor.
- IA de conteo vehicular por cámaras en rutas: **97% de correlación** entre circulación vehicular y ventas de combustible.
- Software de conteo desarrollado internamente por la Gerencia de Tecnología de YPF (no se menciona proveedor externo).
- Algoritmo de micropricing que analiza: indicadores macroeconómicos, inflación, tipo de cambio, demanda, franjas horarias, precios de competidores y diferenciales de precio.
- Resultados: **+35% de rentabilidad nocturna** (período junio-julio), reducción del tiempo de carga de combustible de **5 minutos a menos de 3 minutos** en 3 meses.
- Monitoreo de nivel de tanques en estaciones, logística de abastecimiento optimizada por IA, análisis de satisfacción de clientes vía redes sociales/app.

### 2.3 RTIC Neuquén — Upstream (lmneuquen/mase, agosto 2025)

- **Más de 2.000 pozos** supervisados, **más de 100 instalaciones**, **290 camiones**, **8 equipos de pulling**, demanda eléctrica **>90 MW**.
- **129 personas** en turnos rotativos, **54 puestos de trabajo**.
- **Más de 1,5 millones de variables operativas** procesadas en tiempo real, **más de 150 cámaras**, **más de 420 cuadrillas** coordinadas, **~900 tareas diarias** programadas.
- **13 drones**, con lentes de control por voz y cámara termográfica; **más de 300 recursos de campo** conectados vía antenas Starlink.
- **Nova**: agente de IA que "anticipa desvíos analizando grandes volúmenes de datos operativos" (sin detalle de proveedor/tecnología subyacente — no se menciona vínculo con Azure, Corva o Globant en esta fuente).
- **Argus**: plataforma de **desarrollo propio** ("interno") para visualizar montajes de AIB (Alta/Instalaciones de Bombeo, no aclarado el acrónimo exacto en la fuente) en tiempo real y detectar desvíos antes de que se conviertan en incidentes.
- Inaugurado en **agosto de 2025**, quinto RTIC de YPF, el de mayor tamaño (400 m² según el barrido previo, cifra no repetida textualmente en este fetch pero consistente).

**Corrección de matiz:** no hay evidencia en esta fuente de que Nova o Argus estén construidos sobre plataformas de terceros (Microsoft, Corva, Globant); todo indica desarrollo interno de YPF, aunque la fuente no profundiza en el stack tecnológico subyacente (lenguajes, nube, base de datos).

### 2.4 Acuerdo YPF-Corva (Río Negro, renovación 2026)

- Corva es una plataforma tecnológica de **Houston** encabezada por su Executive Chairman **Dharmesh Mehta**; YPF representada por Horacio Marín.
- La plataforma se describe como un **"sistema operativo digital" para la construcción de pozos**, integrando perforación, fractura e intervención de pozos en un entorno único.
- Objetivo declarado: transición de monitoreo pasivo a **"gestión activa de las operaciones en tiempo real"**, con analítica avanzada e IA integrada.
- Hacia operaciones progresivamente autónomas mediante "asesores digitales", alertas inteligentes, automatización, **copilotos digitales y agentes de IA**.
- **Limitación de la fuente:** no menciona fechas concretas (ni del acuerdo original ni de la renovación puntual), lo que impide fechar con precisión el inicio de la relación YPF-Corva más allá de lo ya reportado en el barrido previo (diciembre de 2024).

### 2.5 Acuerdo YPF-Microsoft / GAIA (ADN Sur)

- **Fecha exacta confirmada: 28 de agosto de 2024.**
- GAIA: chatbot de IA generativa sobre **Azure OpenAI Service**, integrado a **Y-Click!** (plataforma de gestión de contratos y flujo de trabajo con proveedores, con registro electrónico de actividades y facturación).
- Cita textual de **Leandro Masciotta** (líder de Tecnología y Procesos, YPF): el desarrollo de GAIA "se completó en un tiempo récord, logrando un prototipo funcional en solo tres meses."
- Declaraciones de **Fernando López Iervasi** (presidente de Microsoft Sudamérica hispanohablante) y de un operador de mesa de ayuda de Y-Click! sobre el impacto positivo en la gestión de consultas.
- Esta sigue siendo la **única evidencia primaria concreta y fechada** de uso de una nube pública específica (Azure) en el stack de YPF.

### 2.6 Digital Suppl.AI — YPF y Globant (Río Negro)

- **46 agentes de IA especializados**, integrados en **8 soluciones agénticas** (dato nuevo no capturado en el barrido previo: la agrupación en 8 soluciones).
- Áreas: compras (orquestación vía chat con agentes), inventario/stock (trazabilidad de punta a punta sobre "datos fragmentados"), contratos (automatización supervisada por expertos), proveedores (gestión integral de relaciones y base de datos).
- Modelo de Globant: suscripción **"AI Pods"** (agentes de IA supervisados por expertos).
- Fecha de publicación del artículo: **29 de octubre de 2025** (no se especifica fecha exacta de lanzamiento del proyecto, solo que ya estaba en marcha a esa fecha).
- Citas: Horacio Marín (CEO YPF) — "Vamos a lograr que Argentina exporte más de 30.000 millones de dólares para el 2031" (nota: esta fuente dice **2031**, no 2030 como en otros artículos del barrido — posible variación de declaración o error de transcripción, a verificar). Martín Migoya (CEO Globant) expresó orgullo por la colaboración.

### 2.7 RTOR La Plata (Río Negro) — CORRECCIÓN IMPORTANTE

Esta fuente **no confirma** dos cifras clave del finding previo (200.000 variables monitoreadas y 20% de mejora de rentabilidad). En su lugar, el artículo leído completo aporta:
- Infraestructura física: **180 km de fibra óptica**, **16 km de conductos**, **4 km de bandejas de cables**, **20 toneladas de soportes**, **30 tableros de control**.
- El complejo procesa **más de 210.000 barriles de petróleo por día**, 70% proveniente de crudo shale de Vaca Muerta — esta cifra de "210.000" corresponde a **capacidad de refinación**, no a "variables monitoreadas"; es plausible que el barrido previo haya confundido o mezclado esta cifra con la de "200.000 variables" de otra fuente no leída en este pase.
- **Fecha de inauguración: 23 de diciembre de 2025** (no marzo, como sugería el finding previo sobre "un RTIC inaugurado en marzo del mismo año").
- La fuente no menciona el 20% de mejora de rentabilidad ni tecnología específica (solo "de última generación").

**Conclusión:** el finding "RTOR La Plata: 200.000 variables, +20% rentabilidad" del barrido previo **no queda respaldado por esta fuente primaria** y debe tratarse como no verificado o proveniente de otro artículo no confirmado en este pase. Se recomienda tratarlo con cautela en el diseño del proyecto (no usar "200.000 variables" como especificación de esquema sin una segunda fuente).

### 2.8 Toyota Well (La Nación)

- Lanzamiento: **inicios de 2024**.
- Metodología: Toyota Production System (TPS), con principios de mejora continua inspirados en la línea de ensamblaje de Toyota.
- Estructura: **3 ejes de trabajo, 6 frentes integrados, ~100 personas involucradas** (el barrido previo decía "100-250 personas"; esta fuente puntual solo confirma ~100).
- Meta declarada: reducción del **15-30%** del ciclo de construcción de pozos.
- Cita de **Micaela Julieta Cecchini** (Gerente de Agilidad, Innovación y Mejora Continua de YPF): "Con este trabajo vamos a reducir entre 15 y 30% el ciclo de construcción de pozos, migrando hacia un modelo industrializado, como la línea de ensamblaje continuo de Toyota."
- Alcance inicial: **2 líneas prototipo** para prueba y ajuste, con plan de escalado a todo Vaca Muerta.
- Contexto: parte del Plan 4x4, con meta de USD 30.000 millones en exportaciones a 2030 (esta fuente sí dice 2030, consistente con el barrido, a diferencia de la fuente de Globant que decía 2031).

## 3. Tabla comparativa de RTIC/RTOR (cifras confirmadas por lectura completa)

| Centro | Ubicación | Inauguración | Pozos/alcance | Variables/datos | Personal | Cámaras/drones |
|---|---|---|---|---|---|---|
| RTIC Upstream Puerto Madero | Piso 26, Torre YPF, Bs. As. | 13-dic-2024 | 13-14 equipos activos (cap. máx. 16) | 35M datos/pozo; 80-100 variables/KPIs | 88 profesionales, turnos 7×7 | 90 cámaras |
| RTIC Comercialización | Piso 11, Torre YPF, Bs. As. | No confirmado en esta lectura | 1.600+ estaciones, 2.400 camiones | Datos por surtidor; conteo vehicular (97% correlación) | No especificado | Cámaras de tránsito en rutas |
| RTIC Upstream Neuquén | Neuquén capital | Agosto 2025 | 2.000+ pozos, 100+ instalaciones | 1,5M variables en tiempo real | 129 personas, 54 puestos | 150+ cámaras, 13 drones |
| RTOR La Plata | Complejo Industrial La Plata | 23-dic-2025 | Refinería (210.000 bbl/día) | No confirmado (200.000 variables no respaldado en esta fuente) | No especificado | No especificado |

## 4. Findings del barrido previo: confirmados, matizados o descartados

**Confirmados sin cambios:**
- RTIC Puerto Madero, 13-dic-2024, 35M datos/pozo, Starlink 300 Mbps (Starlink no verificado con cifra de Mbps en las 8 fuentes leídas en este pase, viene de fuente distinta del barrido, se mantiene sin cambios).
- Acuerdo YPF-Microsoft/GAIA, 28-ago-2024, Azure OpenAI Service, prototipo en 3 meses.
- Toyota Well, TPS, 15-30% de reducción, cita de Cecchini.
- Digital Suppl.AI, Globant, 46 agentes de IA (se agrega precisión: agrupados en 8 soluciones agénticas).
- RTIC Neuquén: 2.000+ pozos, 1,5M variables, Nova y Argus como plataformas propias.

**Matizados (requieren precisión adicional al usarse en el diseño del proyecto):**
- "80-100 variables" en RTIC Puerto Madero: una fuente dice 80, otra dice "100+ variables y 80+ KPIs" — son probablemente categorías distintas (variables de proceso vs. KPIs calculados), no una contradicción, pero conviene modelarlas como dos tablas distintas en el esquema de datos (raw_variables vs. computed_kpis).
- "Hasta 16 equipos de perforación": es la capacidad de diseño; el número operando en la práctica es 13-14. Para un proyecto de simulación de datos, usar 16 como límite superior de partición pero 13-14 como valor típico de "equipos activos" en los datos sintéticos.
- Toyota Well "100-250 personas": la fuente primaria (La Nación) solo confirma ~100; cifras mayores probablemente vienen de otra fuente de seguimiento no leída en este pase.

**Descartado / no respaldado por esta lectura (usar con cautela):**
- RTOR La Plata "200.000 variables monitoreadas" y "+20% de rentabilidad": **no aparece en el artículo de Río Negro leído completo**, que en cambio da cifras de infraestructura física (fibra óptica, tableros) y capacidad de refinación (210.000 bbl/día). Recomendación: si el proyecto portfolio usa "200.000 variables" como parámetro de un dataset sintético de refinería, aclarar en el README que la cifra proviene de un finding no reconfirmado y no de esta fuente primaria.

## 5. Implicancias para el diseño del proyecto de Data Engineering

Dado que **no hay evidencia de un proveedor de nube confirmado más allá de Azure (solo para GAIA)**, el diseño del proyecto portfolio debe tratarse explícitamente como una **simulación/inspiración basada en cifras públicas de YPF**, no como una réplica de un stack real documentado. Esto es coherente con lo que YA se sabía del barrido previo y se reconfirma aquí: no hay case study oficial de AWS, GCP, Databricks, Snowflake, Palantir, SAP o AVEVA que mencione a YPF.

Sugerencias de diseño basadas en las cifras verificadas:
1. **Ingesta de streaming (upstream):** simular sensores de pozos con las cifras reales de referencia — hasta 1,5M variables/tiempo real (RTIC Neuquén) u 80-100 variables/pozo (RTIC Puerto Madero) según la granularidad elegida; usar Kafka/Kinesis-like en local o Databricks Free Edition con Structured Streaming.
2. **Batch/dimensional (downstream):** dataset de 1.600 estaciones × 2.400 camiones con métricas de ventas, correlacionable con un feature sintético de "conteo vehicular" (97% correlación) para un caso de uso de forecasting de demanda de combustible.
3. **Refinería (La Plata):** dado que la cifra de variables no está confirmada, se recomienda diseñar el dataset de refinería con un número de columnas configurable (parametrizable) y documentar la incertidumbre, en vez de fijar "200.000" como especificación dura.
4. **Capa de IA generativa:** un módulo tipo "GAIA" (chatbot sobre contratos/documentos) es reproducible con LLMs open-source o gratuitos, referenciando el caso real de Azure OpenAI Service como inspiración, sin necesidad de usar Azure realmente.
5. **Gemelo digital / mantenimiento predictivo (Punta Colorada):** al no confirmarse proveedor BIM, se recomienda tratar este módulo como opcional/stretch-goal, no como núcleo del proyecto, hasta obtener más evidencia.

## 6. Preguntas abiertas — no resueltas en este pase (cupo de WebSearch agotado)

Las siguientes 5 búsquedas estaban planificadas pero no pudieron ejecutarse porque la sesión alcanzó su límite de 200 llamadas a WebSearch. Se recomienda repetirlas en una sesión nueva:

1. `YPF AWS Azure "cloud" data lake infraestructura nube RTIC` — para identificar el proveedor de nube que sostiene la infraestructura de datos de los RTIC más allá de GAIA.
2. `YPF AVEVA PI System OSIsoft historian` — para confirmar o descartar el uso de PI System como historian de planta.
3. `YPF SAP S/4HANA ERP contrato` — para confirmar si YPF usa SAP como ERP corporativo (distinto de YPFB Bolivia).
4. `gemelo digital BIM tanques VMOS Punta Colorada YPF proveedor software` — para identificar el proveedor/integrador del gemelo digital BIM.
5. `Y-TEC YPF inteligencia artificial big data Vaca Muerta modelos` — para detallar el rol técnico concreto de Y-TEC en modelos de IA/analítica.

Estas preguntas siguen sin respuesta y deben marcarse como riesgo de diseño: el proyecto portfolio no debe asumir un proveedor de nube, ERP o historian específico como parte del "stack real de YPF" sin volver a intentar esta investigación.

## 7. Fuentes

1. https://www.rionegro.com.ar/energia/como-es-el-megacerebro-de-ypf-que-ya-controla-los-pozos-de-vaca-muerta-3926124/
2. https://www.infobae.com/def/2025/08/23/como-son-los-centros-inteligentes-que-monitorean-en-tiempo-real-vaca-muerta-y-las-estaciones-de-servicio-de-ypf/
3. https://mase.lmneuquen.com/vaca-muerta/ypf-monitorea-mas-2000-pozos-vaca-muerta-su-rtic-ia-drones-y-starlink-n1248621
4. https://www.rionegro.com.ar/energia/ypf-y-corva-renuevan-acuerdo-para-impulsar-la-transformacion-digital-en-vaca-muerta-4705162/
5. https://www.adnsur.com.ar/pulso-energetico/acuerdo-entre-ypf-y-microsoft-para-impulsar-la-gestion-de-contratos-con-inteligencia-artificial_a66cf407c5ae53ef0a7cedc89
6. https://www.rionegro.com.ar/energia/ypf-y-globant-lanzan-un-proyecto-para-acelerar-la-transformacion-digital-con-inteligencia-artificial-4353734/
7. https://www.rionegro.com.ar/energia/ypf-inauguro-la-nueva-sala-real-time-operations-room-en-el-complejo-industrial-la-plata-4412625/
8. https://www.lanacion.com.ar/economia/toyota-well-el-plan-con-el-que-ypf-planea-reducir-el-tiempo-de-construccion-de-pozos-en-vaca-muerta-nid06122024/

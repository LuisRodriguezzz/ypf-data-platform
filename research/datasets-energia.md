# Datasets públicos de datos.energia.gob.ar para un proyecto Data Engineering YPF

## 1. Resumen ejecutivo

El portal `datos.energia.gob.ar` (CKAN, Secretaría de Energía de la Nación) es la fuente primaria correcta para un proyecto portfolio end-to-end sobre YPF. Confirmé por lectura directa de la API `package_show`/`package_search` (vía `curl`, ya que **todo fetch a `https://` retorna 301 hacia el mismo host en `http://`**, y algunas herramientas de fetch quedan en loop de redirect si no se sigue manualmente la URL `http://`) los 53 recursos del dataset núcleo de producción por pozo, el esquema exacto de columnas, los tamaños reales de los CSV anuales grandes, el conteo real de filas del CSV 2006 (748.653 filas de datos, **solo del año 2006**, no acumulado histórico), y el contenido interno real de los ZIP de Reservas (son **XLSX con tablas dinámicas de doble encabezado**, no CSV planos, pero sí tienen columna `OPERADOR` filtrable por YPF). También confirmé que el dataset de Precios EESS (Res. 1104/04) **no** tiene `datastore_active=true` en todos sus recursos como se creía —8 de 29 años están con datastore inactivo— y descubrí un recurso adicional clave no detectado en el barrido previo: **"Producción de Pozos de Gas y Petróleo No Convencional"**, un CSV específico para Vaca Muerta dentro del mismo dataset de producción por pozo, además de dos recursos agregados por yacimiento que resuelven la pregunta abierta sobre el join `areayacimiento`/`idareayacimiento`.

## 2. Dataset núcleo: Producción de petróleo y gas por pozo (Capítulo IV)

- URL de metadatos: `http://datos.energia.gob.ar/api/3/action/package_show?id=produccion-de-petroleo-y-gas-por-pozo`
- Package id: `c846e79c-026c-4040-897f-1ad3543b407c`
- **53 recursos confirmados**, todos con `format: CSV` salvo el shapefile.

### 2.1 Recursos por categoría (confirmados por lectura completa de `resources[]`)

| Categoría | Cantidad | Detalle |
|---|---|---|
| CSV anual "simple" 2006–2026 | 21 | Un recurso por año, ej. `Producción de Pozos de Gas y Petróleo - 2024` (id `43a09dce-1742-44d0-bc13-f193deaab563`, actualizado 2026-03-03) — **nota: 2024 y 2025 aparecen duplicados con dos ids distintos** (uno más viejo y uno con `last_modified` más reciente, p. ej. 2024 tiene `43a09dce...` y `94d82d18...`; 2025 tiene `d774b5d7...` y `6f9f63bd...`) |
| CSV anual "DDJJ abiertas y cerradas" 2006–2026 | 21 | Ej. `Producción de Pozos de Gas y Petróleo - 2024 (DDJJ abiertas y cerradas)` (id `0a352dee-8b4e-4e95-b01e-5b8082ce22ac`) |
| **Producción No Convencional** | 1 | `Producción de Pozos de Gas y Petróleo No Convencional` (CSV, id `b5b58cdc-9e07-41f9-b392-fb9ec68b0725`, actualizado 2026-08-22) — **no detectado en el barrido previo**, clave para el módulo Vaca Muerta |
| Catálogo maestro de pozos | 1 | `Capítulo IV - Pozos` (CSV, id `cb5c0f04-7835-45cd-b982-3e25ca7d7751`, con geometría) |
| Shapefile de pozos | 1 | `Capítulo IV - Pozos` (SHP/ZIP, id `3fcda0c5-68aa-4f33-bbe2-0180e6dbeebe`) |
| Listado de pozos por operadora | 1 | `Listado de pozos cargados por empresas operadoras` (CSV, id `cbfa4d79-ffb3-4096-bab5-eb0dde9a8385`) |
| Series históricas por cuenca | 2 | `Serie histórica de producción de Gas Natural por cuenca y sub tipo de recurso` (id `a3244ddd...`) y su equivalente de petróleo (id `af8c50bb...`) |
| **Agregados por yacimiento** | 2 | `Producción de capítulo IV agrupada por yacimiento y formación productiva` (id `2f2834f4-1981-448f-9a3c-1e519d8c10cd`) y `Producción de Petróleo y Gas (Capítulo IV) por yacimiento y antigüedad de pozo productivo` (id `adf793e7-05a6-449c-bd04-7ca241dfbab5`) |
| Padrón de primera producción | 1 | `Padrón de Pozos de Capítulo IV con fecha de primera producción` (id `5578dd48-d0dd-487e-8ddc-bd3ebb1afef0`) |

Total: 21+21+1+1+1+1+2+2+1 = 51 (más los 2 duplicados de 2024/2025 arriba mencionados = 53). Esto **corrige** la cuenta previa: no son "un CSV por año simple + un CSV DDJJ" limpiamente 1:1 en todos los años; hay años (2024, 2025) con dos recursos "simples" distintos (posible republicación/reprocesamiento), y el resto son recursos adicionales agregados que antes no estaban listados en detalle.

### 2.2 Esquema confirmado del CSV anual (verificado leyendo la cabecera real del archivo 2006 descargado)

Cabecera exacta obtenida con `head` sobre el archivo binario descargado (`prod2006.csv`, primera línea, con BOM UTF-8):

```
idempresa,anio,mes,idpozo,prod_pet,prod_gas,prod_agua,iny_agua,iny_gas,iny_co2,iny_otro,tef,vida_util,
tipoextraccion,tipoestado,tipopozo,observaciones,fechaingreso,rectificado,habilitado,idusuario,empresa,
sigla,formprod,profundidad,formacion,idareapermisoconcesion,areapermisoconcesion,idareayacimiento,
areayacimiento,cuenca,provincia,tipo_de_recurso,proyecto,clasificacion,subclasificacion,sub_tipo_recurso,fecha_data
```

Esto **confirma exactamente** el esquema reportado en el barrido previo (38 columnas, sin la columna final `id` que sí aparecía en el CSV 2025 citado anteriormente — el archivo 2006 termina en `fecha_data`, sin `id`; el esquema puede variar levemente entre años).

### 2.3 Tamaño y filas reales (verificación empírica, no solo `Content-Length`)

Descargué completo el CSV de 2006 (`235.915.154 bytes`, confirmado exacto contra el tamaño reportado por CKAN `size: 235915154`):

- `wc -l` → **748.654 líneas** (1 encabezado + **748.653 filas de datos**).
- Verificación de la columna `anio`: `awk -F, '{print $2}' | sort -u` devuelve **un único valor: `2006`** → **el archivo NO es un acumulado histórico**, contiene únicamente datos de ese año, como se sospechaba pero no estaba confirmado.
- Filtro `empresa == 'YPF S.A.'` (grep exacto sobre el campo): **370.449 filas** de las 748.653 corresponden a YPF S.A. en 2006 (~49%). Esto es consistente con el rol dominante histórico de YPF en pozos de baja productividad heredados de la época estatal.
- El recurso 2024 (`94d82d18-488b-434c-806d-ee5e053ce1cd`) reporta `size: 319898240` y `datastore_active: true` en los metadatos — **dato nuevo**: aunque es un CSV de descarga plana, CKAN también expone `datastore_active=true` para estos recursos anuales grandes, lo que sugiere que **sí podría consultarse vía `datastore_search`/`datastore_search_sql` con filtros server-side** en vez de descargar 300+ MB completos — recomendable validar esto en la fase de diseño técnico del proyecto, ya que cambiaría la arquitectura de ingesta (SQL pushdown vs. descarga completa + filtro en Spark/DuckDB).

## 3. Fractura de pozos (Adjunto IV)

- Package id `71fa2e84-0316-4a1b-af68-7f35e41f58d7`, **solo 2 recursos** (no 1 como se creía): el CSV diario (id `2280ad92-6ed3-403e-a095-50139863ab0d`, actualizado `2026-09-04T04:00:02`) más un **PDF infográfico** (`Infografía Hidrocarburos No Convencionales`, id `7d605931-60c9-49ab-be6c-32433f42bfd4`, apuntando al dominio legacy `www.energia.gob.ar`, sin relevancia para ETL).
- Las `notes` del dataset confirman textualmente el contenido: *"Detalle por pozo, formación, tipo de reservorio, yacimiento, concesión y provincia de: longitud de rama horizontal (m), cantidad de etapas de fractura, tipo de terminación, toneladas de arena bombeada nacional e importada (t), agua inyectada (m3), CO2 inyectado (m3), presión máxima (psi), potencia de equipos de fractura (hp), fechas de inicio y fin de fractura, y empresa informante."* — y aclara: *"Datos preliminares sujetos a revisión."*, dato relevante para documentar calidad de datos en el proyecto (recurso "en vivo", no cerrado/DDJJ).

## 4. Perforación de pozos de petróleo y gas

Package id `7ea2ac77-d7a0-4129-9fbf-6f1a25d94e21`, **21 recursos confirmados**. Desagregaciones por empresa relevantes para YPF:

| Recurso | id |
|---|---|
| Metros perforados por empresa | `3b6b2a2d-7917-4772-a433-216aa07cb86b` |
| Pozos terminados por concepto y empresa desde 2009 | `42c4eafa-4d36-479d-9d54-eae52a5b2f87` |
| Pozos terminados por tipo de pozo y empresa | `284e9bee-1302-4767-a95e-13da87a87726` |
| Pozos en perforación por empresa | `7fcd6c41-9358-4e16-96f1-0380444bff26` |

Además existen desagregaciones equivalentes por cuenca y por provincia, series "ant. a 2009" (legacy), y un único recurso `Pozos y Metros Perforados (Tablas Dinámicas)` en ZIP que apunta al **dominio legacy** `www.energia.gob.ar/.../tablas_dinamicas/upstream/sescoweb_mtsperforados.zip`.

## 5. Precios en Surtidor (Res. 314/2016)

Package id `1c181390-5045-475e-94dc-410429be4b17`, **solo 3 recursos** (no más):

| Recurso | Formato | datastore_active |
|---|---|---|
| Precios vigentes en surtidor - Resolución 314/2016 | CSV | **True** (id `80ac25de-a44a-4445-9215-090cf55cfda5`, el mismo confirmado en el barrido previo) |
| **Precios históricos** | CSV | **True** (id `f8dda0d5-2a9f-4d34-b79b-4e63de3995df`) — **recurso no detectado antes**, con datastore activo, probablemente la serie temporal completa (el recurso "vigentes" parece ser solo el snapshot actual) |
| Aplicaciones | HTML | False — apunta a `https://preciosensurtidor.energia.gob.ar/`, el front-end oficial del dataset |

Esto es un hallazgo importante: el proyecto debería usar **ambos** recursos datastore (vigentes + históricos) para tener series temporales, no solo el de "vigentes".

## 6. Precios y volúmenes EESS (Res. 1104/04) — corrección importante

Package id `708f9ab4-829b-4f02-b507-f303c5bc4800`, **29 recursos confirmados**, pero **contradice el finding previo** de que "todos con datastore_active=true". Verificado campo por campo:

| Año/recurso | datastore_active |
|---|---|
| Desde Diciembre2024, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016 | **True** |
| 2015 | **False** |
| 2014 | True |
| 2013, 2012 | **False** |
| 2011, 2010 | True |
| 2009, 2005, 2008, 2007, 2006 | **False** |
| 2004 | True |
| Precio final con impuestos en EESS, Volúmenes en EESS, Precios combustibles por período (ARS/litro), Ventas a vehículos con chapa patente extranjera desde 2015, EESS y Operadores | True |
| Shapefile de EESS y Operadores | False (no aplica, es SHP) |

**8 de los 29 recursos no tienen datastore activo** (años 2005-2009, 2012, 2013, 2015): para esos años el acceso es solo por descarga CSV directa, no por `datastore_search` con filtros SQL. Esto afecta el diseño de ingesta: la arquitectura debe soportar **dos rutas de extracción** (API datastore para años recientes, descarga CSV+parseo para años legacy).

## 7. Reservas de Petróleo y Gas — hallazgo crítico sobre formato interno

Package `reservas-de-petroleo-y-gas`, **40 recursos**, notas del dataset: *"Datos de Reservas Comprobadas, Probables, posibles y recursos de Petróleo y Gas. Por cuenca, provincia y yacimiento y concesión."*

**Descargué y abrí realmente uno de los ZIP** (`reservas_al_31-12-2024.zip`, resuelto vía redirect 302 desde la URL CKAN hacia el dominio legacy `www.energia.gob.ar/contenidos/archivos/.../reservas/reservas_al_31-12-2024.zip`, 314.161 bytes comprimido / 415.774 bytes descomprimido). Contenido real:

- **No es CSV, es un único archivo XLSX** (`reservas al 31-12-2024.xlsx`) con **2 hojas**: `fin de concesión` y `fin de vida útil`.
- Estructura de **tabla dinámica con encabezado de 7 filas fusionadas**: fila 3 distingue `CONVENCIONAL` / `NO CONVENCIONAL` / `CONVENCIONAL + NO CONVENCIONAL`; fila 4 distingue `RESERVAS` / `RECURSOS CONTINGENTES`; fila 5 distingue `COMPROBADAS`/`PROBABLES`/`POSIBLES`; fila 6 distingue `PET` (Mm3) / `GAS` (MMm3); fila 7 son las unidades.
- **Columnas de identificación confirmadas (fila 7, primeras 5 celdas): `OPERADOR, CUENCA, PROVINCIA, CONCESIÓN O PERMISO, YACIMIENTO`.**
- Fila de ejemplo real: `('ALIANZA PETROLERA ARGENTINA S.A.', 'GOLFO SAN JORGE', 'Santa Cruz', 'ESTANCIA LA MARIPOSA', 'ESTANCIA LA MARIPOSA', 1.7, 81.6, 2.8, 117.5, ...)`.

**Esto responde la pregunta abierta**: sí es filtrable por operador/empresa (columna `OPERADOR` textual, análoga a `empresa` en producción por pozo — se debe filtrar por `'YPF S.A.'`), pero el ETL debe manejar un **parser de Excel con encabezado jerárquico de 7 filas**, no un CSV tabular simple — esto es una pieza de ingeniería no trivial (pivotar/despivotar columnas ANCHO→LARGO con `pandas`/`openpyxl` o `polars` antes de cargar a un modelo relacional). El dataset abarca 2004–2024 en incrementos anuales, con boletines PDF de consolidación intercalados (1998-2016, 2009-2018, 2008-2017) que NO tienen datos tabulares descargables.

## 8. Catálogo completo relacionado a "producción" (64 datasets)

`package_search?q=produccion&rows=50` devuelve `count: 64`. Datasets adicionales de interés no cubiertos en el barrido previo, todos confirmados existentes con su slug real:

| Dataset | slug |
|---|---|
| Producción de Petróleo desde 1950 | `produccion-de-petroleo-desde-1950` |
| Producción Gas Natural desde 1950 | `produccion-gas-natural-desde-1950` |
| Producción de GLP / por planta | `produccion-de-glp`, `produccion-de-glp-por-planta-` |
| Producción de combustibles | `produccion-de-combustibles` |
| Pronósticos de Producción de Petróleo y Gas (Tablas Dinámicas) | `pronosticos-de-produccion-de-petroleo-y-gas-tablas-dinamicas` |
| Producción de Petróleo y Gas (SESCO, Tablas Dinámicas) | `produccion-de-petroleo-y-gas-tablas-dinamicas` |
| Producción hidrocarburos - Lotes de Explotación | `produccion-hidrocarburos-lotes-de-explotacion` |
| Producción hidrocarburos - Concesiones de Explotación | `produccion-hidrocarburos-concesiones-de-explotacion` |
| Producción hidrocarburos - Puntos de Venteo Declarados | `produccion-hidrocarburos-puntos-de-venteo-declarados` |
| Producción Hidrocarburos - Yacimientos Según Profundidad Promedio | `produccion-hidrocarburos-yacimientos-segun-profundidad-promedio` |
| **Trayectorias de Pozo Vaca Muerta** | `trayectoria-de-pozos` |
| Registro de empresas de hidrocarburos del upstream | `registro-de-empresas-de-hidrocarburos-del-upstream` |
| Evaluación de Formaciones en la Argentina | `evaluacion-de-formaciones-en-la-argentina` |
| Mapas de Exploración y Explotación de Hidrocarburos | `mapas-de-explortacion-y-produccion-de-hidrocarburos` |
| Exploración de hidrocarburos - Permisos, Sísmicas 3D, Líneas Sísmicas 2D | `exploracion-hidrocarburos-permisos-de-exploracion`, `exploracion-hidrocarburos-sismicas-3d`, `exploracion-hidrocarburos-lineas-sismicas-2d` |

El más relevante nuevo para el proyecto es **`trayectoria-de-pozos` ("Trayectorias de Pozo Vaca Muerta")**, no detectado en el barrido previo, potencialmente clave para el módulo de visualización geoespacial de no convencional. No alcancé a leer su `package_show` en detalle por límite de presupuesto de esta pasada; queda como recomendación de siguiente lectura.

## 9. Producción de hidrocarburos - Yacimientos

Package id `7378520e-4d10-48a9-92e9-7e20e69a8277`, notas: *"Ubicación y polígonos de yacimientos."* Confirmado **2 recursos**: CSV (`produccin-hidrocarburos-yacimientos.csv`) y SHP/ZIP, ambos con URL de descarga directa sobre el dominio CKAN (no legacy). No expone columnas de producción convencional/no convencional explícitas en el nombre; el join hacia producción por pozo se hace por `areayacimiento`/`idareayacimiento`, confirmado en la sección 2.1 mediante los dos recursos agregados por yacimiento del dataset principal (`2f2834f4...` y `adf793e7...`), que ya traen la agregación resuelta sin necesidad de reconstruirla manualmente.

## 10. Precio de exportación / comercio exterior — sin serie de gas en boca de pozo

Confirmé leyendo `package_show?id=precio-de-exportacion-de-petroleo-crudo`: **un único recurso XLSX** (`precio-exportacion-crudo.xlsx`, dominio legacy `www.energia.gob.ar`), exclusivamente de petróleo crudo. `package_search?q=precio exportacion` devuelve 5 datasets (`precio-de-exportacion-de-petroleo-crudo`, `glp` con paridad de exportación, `precios-de-biodiesel`, `precios-de-comercio-exterior2`, `refinacion-y-comercializacion-de-petroleo-gas-y-derivados-tablas-dinamicas`) — **ninguno contiene serie de "precio de gas natural en boca de pozo"**. Se confirma que este dato **no existe** como dataset nombrado en el portal; si se necesita para el proyecto, debería aproximarse indirectamente (p. ej. con series de `precios-de-escalante` para petróleo, o quedar fuera de alcance para gas).

## 11. Portal general datos.gob.ar — relación con datos.energia.gob.ar

Consulté `datos.gob.ar/api/3/action/package_search?q=YPF&rows=20` (API CKAN del portal federado nacional): **count=4**, todos pertenecientes a `organization: "Secretaría de Energía"` (`geologia-climatologia-isobatas`, `comercializacion-hidrocarburos-combustibles-liquidos-aeroplantas`, `evaluacion-de-formaciones-en-la-argentina`, `deteccion-satelital-de-venteos`). Esto confirma que **datos.gob.ar federa (republica metadatos de) el mismo catálogo de la Secretaría de Energía**, sin agregar datasets propios adicionales sobre YPF ni series de precio distintas. No es necesario tratar ambos portales como fuentes independientes; alcanza con `datos.energia.gob.ar` como fuente única, y opcionalmente `datos.gob.ar` como espejo/backup de metadatos.

## 12. Implicancias para el diseño del proyecto de Data Engineering

1. **Arquitectura de ingesta mixta**: para producción por pozo (300+ MB/año) conviene evaluar `datastore_search_sql` (dado `datastore_active=true` confirmado también en recursos CSV grandes) para pushdown de filtro `empresa='YPF S.A.'` antes de descargar, evitando mover ~300 MB por año cuando YPF representa una fracción del total.
2. **Dos pipelines de calidad distintos**: (a) recursos con datastore activo → ingestar vía API paginada; (b) recursos legacy sin datastore (EESS años 2005-2009/2012/2013/2015, Reservas XLSX, Perforación "tablas dinámicas") → requieren parser de archivo (CSV/XLSX) con normalización de encabezados jerárquicos.
3. **Reservas requiere un transformador ETL específico** (despivotar encabezado de 7 filas, columnas `OPERADOR/CUENCA/PROVINCIA/CONCESIÓN/YACIMIENTO` + matriz Convencional/No Convencional × Reservas/Recursos Contingentes × Comprobadas/Probables/Posibles × PET/GAS) — buen caso de uso para demostrar dominio de transformación de datos semi-estructurados.
4. **Producción No Convencional** como recurso propio (no solo un filtro de `tipo_de_recurso` dentro del CSV general) simplifica el módulo Vaca Muerta.
5. Confirmar el año 2006 sin acumulado histórico valida que el pipeline de años históricos (2006-2026) puede paralelizarse por año sin riesgo de duplicar datos entre archivos.

## 13. Preguntas que quedan abiertas

- Contenido exacto de `trayectoria-de-pozos` (Trayectorias de Pozo Vaca Muerta) — no leído en profundidad.
- Si `datastore_search_sql` realmente acepta filtros server-side sobre los recursos CSV anuales grandes de producción por pozo (solo se confirmó `datastore_active=true` en metadatos, no se probó la query real).
- Esquema completo de `produccion-hidrocarburos-yacimientos.csv` (solo se confirmó la URL, no se leyó el contenido).

## Fuentes

- https://datos.energia.gob.ar/api/3/action/package_show?id=produccion-de-petroleo-y-gas-por-pozo (redirige a http://datos.energia.gob.ar/... )
- http://datos.energia.gob.ar/api/3/action/package_show?id=produccion-de-petroleo-y-gas-por-pozo
- http://datos.energia.gob.ar/api/3/action/package_show?id=datos-de-fractura-de-pozos-adjunto-iv
- http://datos.energia.gob.ar/api/3/action/package_show?id=perforacion-de-pozos-de-petroleo-y-gas
- http://datos.energia.gob.ar/api/3/action/package_show?id=precios-en-surtidor
- http://datos.energia.gob.ar/api/3/action/package_show?id=precios-eess---resolucion-1104-04
- http://datos.energia.gob.ar/api/3/action/package_show?id=reservas-de-petroleo-y-gas
- http://datos.energia.gob.ar/api/3/action/package_search?q=produccion&rows=50
- http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/4e1c55e5-1f1b-4fc8-aa37-2080d9795f29/download/produccin-de-pozos-de-gas-y-petrleo-2006.csv (descargado completo, 235.915.154 bytes, 748.654 líneas)
- http://datos.energia.gob.ar/api/3/action/package_show?id=produccion-hidrocarburos-yacimientos
- http://datos.energia.gob.ar/api/3/action/package_show?id=precio-internacional-pi-del-petroleo-crudo
- http://datos.energia.gob.ar/api/3/action/package_show?id=precio-de-exportacion-de-petroleo-crudo
- http://datos.energia.gob.ar/api/3/action/package_search?q=precio%20exportacion
- http://www.energia.gob.ar/contenidos/archivos/Reorganizacion/informacion_del_mercado/mercado_hidrocarburos/informacion_estadistica/reservas/reservas_al_31-12-2024.zip (descargado y abierto: XLSX con hojas "fin de concesión" / "fin de vida útil")
- https://datos.gob.ar/api/3/action/package_search?q=YPF&rows=20

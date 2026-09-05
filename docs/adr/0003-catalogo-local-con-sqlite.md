# ADR 0003 — El catálogo Iceberg local usa SQLite como backend

**Estado:** aceptada · 2026-09-05

## Contexto

El catálogo Iceberg REST necesita una base JDBC para sus metadatos. La imagen oficial `apache/iceberg-rest-fixture` incluye el driver de SQLite y no el de Postgres (verificado abriendo el jar). Usar Postgres obligaría a construir y mantener una imagen propia.

## Decisión

El catálogo local usa SQLite sobre un volumen persistente. Postgres queda para los metadatos que son código nuestro (manifiesto de ingesta) y para Airflow.

El foco del proyecto es la plataforma de datos y su despliegue en la nube, no la operación de infraestructura on-premise. En AWS el catálogo es Glue Data Catalog, administrado; este componente no viaja a la nube, así que no justifica inversión.

## Consecuencias

- Dos almacenes de metadatos en local (SQLite para el catálogo, Postgres para lo nuestro). Se acepta a cambio de no mantener imágenes propias.
- Si alguna vez hace falta consultar el catálogo desde SQL, se hace por su API REST, no por la base.

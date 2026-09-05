# ADR 0002 — DuckDB como motor de consulta local, Athena en AWS

**Estado:** aceptada · 2026-09-05

## Contexto

La máquina de desarrollo tiene 16 GB de RAM. Trino aportaría paridad de dialecto con Athena, pero cuesta unos 2 GB de RAM permanentes y un servicio más que operar.

## Decisión

La etapa 1 usa DuckDB embebido para consultas, tests y CI (lee Iceberg y Parquet en MinIO directo). En AWS el motor es Athena. Cuando entre dbt, usa `dbt-duckdb` en local y `dbt-athena` en AWS. Trino se agrega solo en un perfil aparte si se decide demostrar un motor distribuido.

## Consecuencias

- Las diferencias de dialecto entre DuckDB y Athena se aíslan en macros de dbt.
- Se documenta como decisión: herramienta embebida en local, servicio administrado en la nube.

# YPF Data Platform

![CI](https://github.com/LuisRodriguezzz/ypf-data-platform/actions/workflows/ci.yml/badge.svg)

Plataforma de datos end-to-end del upstream argentino sobre datos públicos reales (Secretaría de Energía, 2006-2026) con telemetría real de pozos (Petrobras 3W) y detección de anomalías. Proyecto portfolio orientado a producción: lakehouse medallion en Iceberg, streaming con Kafka y Spark, calidad de datos con contratos, orquestación con Airflow, IaC con Terraform y CI en GitHub Actions.

Estado: semana 0 completada (ver `docs/semana-0-derisking.md`). Ingesta, bronze, silver y **gold** corriendo, orquestados por Airflow (`orchestration/README.md`). Fuentes cerradas de punta a punta, en local y en AWS: `produccion_pozo` y `fractura` (perfil de cada una en `docs/fuentes/`). Gold es un modelo dimensional en dbt (`pipelines/dbt/`, ADR 0009): cuatro dimensiones —`dim_pozo` es SCD tipo 2 sobre 21 años de declaraciones—, tres tablas de hechos y un mart de features para ML, con tests y documentación por columna. Decisiones de arquitectura en `docs/adr/`.

Un stack, dos destinos (ADR 0001): el mismo código de `pipelines/` corre en local sobre Docker Compose (MinIO, Iceberg REST, Spark, Airflow) y en AWS sobre S3, Glue Data Catalog, Glue jobs, Step Functions y Athena (`infra/terraform/README.md`). Lo único que cambia entre destinos es la configuración.

Trazabilidad de datos: cada tabla declara `data_origin` con valores `real`, `simulated` o `derived`.

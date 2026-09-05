# YPF Data Platform

![CI](https://github.com/LuisRodriguezzz/ypf-data-platform/actions/workflows/ci.yml/badge.svg)

Plataforma de datos end-to-end del upstream argentino sobre datos públicos reales (Secretaría de Energía, 2006-2026) con telemetría real de pozos (Petrobras 3W) y detección de anomalías. Proyecto portfolio orientado a producción: lakehouse medallion en Iceberg, streaming con Kafka y Spark, calidad de datos con contratos, orquestación con Airflow, IaC con Terraform y CI en GitHub Actions.

Estado: semana 0 completada (ver `docs/semana-0-derisking.md`). Ingesta, bronze y silver corriendo, orquestados por Airflow (`orchestration/README.md`). Decisiones de arquitectura en `docs/adr/`.

Un stack, dos destinos (ADR 0001): el mismo código de `pipelines/` corre en local sobre Docker Compose (MinIO, Iceberg REST, Spark, Airflow) y en AWS sobre S3, Glue Data Catalog, Glue jobs, Step Functions y Athena (`infra/terraform/README.md`). Lo único que cambia entre destinos es la configuración.

Trazabilidad de datos: cada tabla declara `data_origin` con valores `real`, `simulated` o `derived`.

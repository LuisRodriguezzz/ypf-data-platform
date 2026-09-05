# ADR 0001 — Un stack, dos destinos

**Estado:** aceptada · 2026-09-05

## Contexto

El proyecto debe correr en local de forma permanente y demostrar despliegue cloud. Las plataformas gratuitas tienen límites verificados: Databricks Free Edition restringe el egress y no permite almacenamiento propio; el AWS free account plan cierra la cuenta a los 6 meses.

## Decisión

Un único código de transformación (Python, Spark, Iceberg, dbt) parametrizado por destino.

- Destino `local`: Docker Compose con MinIO, Spark, Iceberg REST catalog, Airflow y DuckDB.
- Destino `aws`: S3, Glue Data Catalog, Athena, Lambda y Step Functions, desplegado con Terraform y destruido al terminar la ventana de créditos.
- Databricks queda fuera del stack; como mucho recibe gold publicado por un script.

## Consecuencias

- Si una línea de transformación difiere entre destinos, el diseño está mal: la diferencia vive en `config/*.env` e `infra/`.
- Orquestación: Airflow local dispara también los jobs de AWS; Step Functions solo en la demo.
- Streaming (Kafka) vive solo en local.

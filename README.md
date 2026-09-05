# YPF Data Platform

Plataforma de datos end-to-end del upstream argentino sobre datos públicos reales (Secretaría de Energía, 2006-2026) con telemetría real de pozos (Petrobras 3W) y detección de anomalías. Proyecto portfolio orientado a producción: lakehouse medallion en Iceberg, streaming con Kafka y Spark, calidad de datos con contratos, orquestación con Airflow, IaC con Terraform y CI en GitHub Actions.

Estado: semana 0 completada (ver `docs/semana-0-derisking.md`). Decisiones de arquitectura en `docs/adr/`.

Trazabilidad de datos: cada tabla declara `data_origin` con valores `real`, `simulated` o `derived`.
